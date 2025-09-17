"""Core WebChannel implementation."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
from dataclasses import dataclass
from enum import Enum
from http.cookiejar import CookieJar
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

import httpx

from .errors import WebChannelError
from .events import EventType, EventTarget, MessageEvent, get_stat_event_target, Event, Stat, StatEvent
from .httpcors import generate_encoded_http_headers_overwrite_param
from .options import WebChannelOptions
from .wire import LATEST_CHANNEL_VERSION, RAW_DATA_KEY, QueuedMap, WireV8

LOGGER = logging.getLogger(__name__)
_DEBUG_ENABLED = os.getenv("WEBCHANNEL_DEBUG", "").lower() not in {"", "0", "false", "no"}


class ChannelState(str, Enum):
    INIT = "init"
    OPENING = "opening"
    OPENED = "opened"
    CLOSED = "closed"


@dataclass(slots=True)
class _PostResponse:
    arrays_outstanding: int
    last_array_id: int
    outstanding_bytes: int


class WebChannel(EventTarget):
    """Python port of the Firebase WebChannel transport."""

    def __init__(
        self,
        url: str,
        options: WebChannelOptions,
        *,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        super().__init__()
        self._url = url
        self._base_url = url
        self._options = options.copy()
        self._client = http_client or httpx.AsyncClient()
        self._wire = WireV8()
        self._state = ChannelState.INIT
        self._sid: Optional[str] = None
        self._host_prefix: Optional[str] = None
        self._http_session_id_param: Optional[str] = (
            self._options.http_session_id_param
        )
        self._http_session_id_value: Optional[str] = None
        self._channel_version = LATEST_CHANNEL_VERSION
        self._client_version = 22
        self._server_version = 0
        self._next_map_id = 0
        self._next_rid = 0
        self._next_t = 1
        self._handshake_queue: List[QueuedMap] = []
        self._last_array_id = 0
        self._acknowledged_array_id = -1
        self._last_post_response_array_id = 0
        self._enable_streaming = not self._options.force_long_polling
        self._detect_buffering_proxy = self._options.detect_buffering_proxy
        self._long_polling_timeout = self._options.long_polling_timeout
        self._forward_timeout_ms = int(
            self._options.internal_channel_params.get("forwardChannelRequestTimeoutMs", 20000)
            if self._options.internal_channel_params
            else 20000
        )
        self._backchannel_timeout_ms = max(self._forward_timeout_ms, 45000)
        self._backchannel_task: Optional[asyncio.Task[None]] = None
        self._backchannel_started = False
        self._closed = False
        self._message_headers = dict(self._options.message_headers or {})
        self._init_headers = dict(self._options.init_message_headers or {})
        self._initial_headers_sent = False
        self._stats_target = get_stat_event_target()
        self._stats_emitted = False
        self._close_lock = asyncio.Lock()
        self._debug_enabled = _DEBUG_ENABLED
        self._cookie_header: Optional[str] = None
        self._fetch_headers = dict(self._options.fetch_headers or {})
        self._session_id_placeholder = "gsessionid"

    # ------------------------------------------------------------------
    async def open(self) -> None:
        if self._state != ChannelState.INIT:
            return
        self._state = ChannelState.OPENING
        self._log("handshake:start", url=self._base_url)
        await self._perform_handshake()

    async def send(self, message: Any) -> None:
        payload = self._normalize_outgoing_message(message)
        queued = QueuedMap(self._next_map_id, payload)
        self._next_map_id += 1
        if self._state == ChannelState.INIT:
            self._handshake_queue.append(queued)
            return
        if self._state != ChannelState.OPENED or self._sid is None:
            raise RuntimeError("WebChannel is not open")
        await self._post_maps([queued])

    async def close(self) -> None:
        async with self._close_lock:
            if self._state == ChannelState.CLOSED:
                return
            self._closed = True
            self._state = ChannelState.CLOSED
            if self._sid is not None:
                await self._send_terminate()
            if self._backchannel_task:
                self._backchannel_task.cancel()
                try:
                    await self._backchannel_task
                except asyncio.CancelledError:
                    pass
            self._backchannel_started = False
            await self._client.aclose()
            self.dispatch_event(EventType.CLOSE, None)

    # ------------------------------------------------------------------
    async def _perform_handshake(self) -> None:
        params = self._build_base_params()
        rid = self._consume_rid()
        params.update(
            {
                "RID": str(rid),
                "CVER": str(self._client_version),
            }
        )

        body, headers = self._build_handshake_payload()
        headers.setdefault("X-Client-Protocol", "webchannel")
        headers.setdefault("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
        self._apply_fetch_headers(headers)
        content_bytes = body.encode()
        url = self._build_url(self._base_url, params)

        self._log(
            "handshake:request",
            method="POST",
            url=url,
            headers=self._sanitize_headers(headers),
            body_preview=body[:2048],
        )

        async with self._client.stream(
            "POST",
            url,
            content=content_bytes,
            headers=headers,
            timeout=self._forward_timeout_ms / 1000,
        ) as response:
            await self._apply_control_headers(response)
            self._log(
                "handshake:response",
                status=response.status_code,
                headers=self._sanitize_headers(dict(response.headers)),
            )
            self._refresh_cookie_header()
            if response.status_code >= 400:
                text = await response.aread()
                self._log(
                    "handshake:error",
                    status=response.status_code,
                    body=text[:2048].decode(errors="replace"),
                )
                raise WebChannelError(
                    status=str(response.status_code),
                    message=text.decode(errors="replace"),
                )

            buffer = ""
            async for chunk in response.aiter_text():
                buffer += chunk
                messages, buffer = self._extract_chunks(buffer)
                for message in messages:
                    await self._handle_incoming_chunk(message)

        if self._state != ChannelState.OPENED:
            raise RuntimeError("WebChannel handshake failed")
        self._handshake_queue.clear()
        self._log(
            "handshake:complete",
            sid=self._sid,
            host_prefix=self._host_prefix,
            gsessionid=self._http_session_id_value,
        )

    async def _handle_incoming_chunk(self, chunk: str) -> None:
        try:
            payload = self._wire.decode_message(chunk)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            LOGGER.warning("Failed to decode WebChannel message: %s", exc)
            return

        if not isinstance(payload, list):
            return

        for entry in payload:
            if not isinstance(entry, list) or len(entry) < 2:
                continue
            array_id, content = entry[0], entry[1]
            self._last_array_id = array_id
            self._log("chunk", array_id=array_id, content=content)
            if self._state == ChannelState.OPENING:
                await self._handle_handshake_payload(content)
            elif self._state == ChannelState.OPENED:
                await self._handle_channel_payload(content)

    async def _handle_handshake_payload(self, content: Any) -> None:
        if isinstance(content, list) and content:
            tag = content[0]
            self._log("handshake:payload", payload=content)
            if tag == "c":
                self._sid = content[1]
                self._host_prefix = content[2]
                if len(content) > 3 and content[3] is not None:
                    self._channel_version = int(content[3])
                if len(content) > 4 and content[4] is not None:
                    self._server_version = int(content[4])
                if len(content) > 5 and content[5]:
                    self._backchannel_timeout_ms = int(1.5 * float(content[5]))
                self._acknowledged_array_id = self._last_array_id
                LOGGER.debug(
                    "webchannel handshake complete sid=%s ver=%s host=%s",
                    self._sid,
                    self._channel_version,
                    self._host_prefix,
                )
                self._state = ChannelState.OPENED
                self.dispatch_event(EventType.OPEN, None)
                self._ensure_backchannel()
            elif tag in ("stop", "close"):
                await self.close()
        else:
            await self._handle_channel_payload(content)

    async def _handle_channel_payload(self, content: Any) -> None:
        if isinstance(content, list) and content:
            tag = content[0]
            if tag == "noop":
                return
            if tag in ("stop", "close"):
                await self.close()
                return
            if tag == "d":
                for item in content[1:]:
                    decoded = self._decode_message_item(item)
                    if decoded is not None:
                        self.dispatch_event(EventType.MESSAGE, MessageEvent(decoded))
                return
        event = MessageEvent(content)
        self.dispatch_event(EventType.MESSAGE, event)

    def _decode_message_item(self, item: Any) -> Any:
        if isinstance(item, dict):
            raw = item.get(RAW_DATA_KEY)
            if isinstance(raw, str):
                try:
                    decoded = json.loads(raw)
                except json.JSONDecodeError:  # pragma: no cover - defensive
                    LOGGER.debug("Failed to decode __data__ payload", exc_info=True)
                    return None
                if isinstance(decoded, dict):
                    return decoded
                return {"data": decoded}
            state_map = item.get("__sm__")
            if isinstance(state_map, dict):
                for value in state_map.values():
                    if isinstance(value, dict):
                        return value
                return None
            return item
        return None

    async def _post_maps(self, maps: Sequence[QueuedMap]) -> None:
        params = self._build_base_params()
        rid = self._consume_rid()
        aid = self._acknowledged_array_id if self._acknowledged_array_id >= 0 else -1
        params.update(
            {
                "SID": self._sid or "",
                "RID": str(rid),
                "AID": str(aid),
                "CVER": str(self._client_version),
            }
        )
        body = self._wire.encode_message_queue(maps, len(maps))
        headers = self._build_message_headers()
        headers.pop("Content-Type", None)
        headers["Content-Type"] = "application/x-www-form-urlencoded;charset=UTF-8"
        self._apply_persistent_headers(headers)
        self._apply_fetch_headers(headers)
        if "Cookie" not in headers:
            self._log("forward:missing_cookie", cookies=str(self._client.cookies))
        url = self._build_url(self._base_url, params)

        self._log(
            "forward:request",
            url=url,
            headers=self._sanitize_headers(headers),
            body_preview=body[:2048],
        )
        async with self._client.stream(
            "POST",
            url,
            content=body.encode(),
            headers=headers,
            timeout=self._forward_timeout_ms / 1000,
        ) as response:
            if response.status_code >= 400:
                text = await response.aread()
                self._log(
                    "forward:error",
                    status=response.status_code,
                    body=text[:2048].decode(errors="replace"),
                )
                raise WebChannelError(
                    status=str(response.status_code),
                    message=text.decode(errors="replace"),
                )

            buffer = ""
            async for chunk in response.aiter_text():
                buffer += chunk
                messages, buffer = self._extract_chunks(buffer)
                for message in messages:
                    await self._handle_post_response(message)
            self._log("forward:response", status=response.status_code)
        self._ensure_backchannel()

    async def _handle_post_response(self, chunk: str) -> None:
        try:
            payload = self._wire.decode_message(chunk)
        except json.JSONDecodeError as exc:  # pragma: no cover
            LOGGER.warning("Failed to decode POST response: %s", exc)
            return

        if isinstance(payload, list) and len(payload) == 3 and isinstance(payload[0], int):
            arrays_outstanding = payload[0]
            self._last_post_response_array_id = payload[1]
            outstanding_bytes = payload[2]
            self._acknowledged_array_id = self._last_post_response_array_id
            if arrays_outstanding == 0:
                return
            LOGGER.debug(
                "Outstanding backchannel arrays=%s bytes=%s",
                arrays_outstanding,
                outstanding_bytes,
            )
            if self._detect_buffering_proxy and not self._stats_emitted:
                self._stats_target.dispatch_event(Event.STAT_EVENT, StatEvent(Stat.PROXY))
                self._stats_emitted = True
        elif isinstance(payload, list):
            # Forward unexpected array payloads to the normal handler.
            for entry in payload:
                if isinstance(entry, list) and len(entry) >= 2:
                    await self._handle_channel_payload(entry[1])

    async def _run_backchannel(self) -> None:
        while not self._closed and self._state == ChannelState.OPENED:
            params = self._build_base_params()
            aid = self._acknowledged_array_id if self._acknowledged_array_id >= 0 else -1
            params.update(
                {
                    "RID": "rpc",
                    "SID": self._sid or "",
                    "AID": str(aid),
                    "CI": "0" if self._enable_streaming else "1",
                    "TYPE": "xmlhttp",
                    "CVER": str(self._client_version),
                }
            )
            if not self._enable_streaming and self._long_polling_timeout:
                params["TO"] = str(self._long_polling_timeout)

            url = self._build_url(self._base_url, params)
            try:
                headers = self._build_message_headers()
                headers.pop("Authorization", None)
                self._apply_persistent_headers(headers)
                self._apply_fetch_headers(headers)
                if "Cookie" not in headers:
                    self._log("backchannel:missing_cookie", cookies=str(self._client.cookies))
                self._log(
                    "backchannel:request",
                    url=url,
                    headers=self._sanitize_headers(headers),
                )
                async with self._client.stream(
                    "GET",
                    url,
                    headers=headers,
                    timeout=self._backchannel_timeout_ms / 1000,
                ) as response:
                    await self._apply_control_headers(response)
                    self._log(
                        "backchannel:response",
                        status=response.status_code,
                        headers=self._sanitize_headers(dict(response.headers)),
                    )
                    if response.status_code >= 400:
                        text = await response.aread()
                        self._log(
                            "backchannel:error",
                            status=response.status_code,
                            body=text[:2048].decode(errors="replace"),
                        )
                        raise WebChannelError(
                            status=str(response.status_code),
                            message=text.decode(errors="replace"),
                        )

                    buffer = ""
                    async for chunk in response.aiter_text():
                        if self._detect_buffering_proxy and not self._stats_emitted:
                            self._stats_target.dispatch_event(
                                Event.STAT_EVENT, StatEvent(Stat.NOPROXY)
                            )
                            self._stats_emitted = True
                        buffer += chunk
                        messages, buffer = self._extract_chunks(buffer)
                        for message in messages:
                            await self._handle_incoming_chunk(message)
                self._refresh_cookie_header()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("Backchannel error: %s", exc)
                self._log("backchannel:exception", error=str(exc))
                self.dispatch_event(EventType.ERROR, exc)
                await asyncio.sleep(1.0)
        self._backchannel_started = False

    def _ensure_backchannel(self) -> None:
        if self._backchannel_started or self._closed or self._state != ChannelState.OPENED:
            return
        self._backchannel_started = True
        self._backchannel_task = asyncio.create_task(self._run_backchannel())

    async def _send_terminate(self) -> None:
        params = self._build_base_params()
        params.update(
            {
                "SID": self._sid or "",
                "RID": str(self._consume_rid()),
                "TYPE": "terminate",
                "CVER": str(self._client_version),
            }
        )
        url = self._build_url(self._base_url, params)
        self._log("terminate:request", url=url)
        try:
            headers = self._build_message_headers()
            self._apply_persistent_headers(headers)
            await self._client.post(url, content=b"", headers=headers, timeout=5.0)
        except httpx.HTTPError:
            LOGGER.debug("Failed to send terminate request", exc_info=True)

    # ------------------------------------------------------------------
    def _normalize_outgoing_message(self, message: Any) -> Dict[str, Any]:
        if isinstance(message, dict):
            if self._options.send_raw_json:
                return {RAW_DATA_KEY: json.dumps(message)}
            return message
        if self._options.send_raw_json:
            if not isinstance(message, str):
                message = json.dumps(message)
            return {RAW_DATA_KEY: message}
        if isinstance(message, str):
            return {RAW_DATA_KEY: message}
        raise TypeError("WebChannel only supports string or dict payloads")

    def _build_handshake_payload(self) -> tuple[str, Dict[str, str]]:
        combined_headers = {**self._message_headers, **self._init_headers}
        handshake_maps = list(self._handshake_queue)
        body = self._wire.encode_message_queue(handshake_maps, len(handshake_maps))
        headers: Dict[str, str] = {}

        if combined_headers and self._options.encode_init_message_headers:
            encoded = generate_encoded_http_headers_overwrite_param(combined_headers)
            body = f"headers={encoded}&{body}"
        else:
            headers = combined_headers

        return body, headers

    def _build_message_headers(self) -> Dict[str, str]:
        return dict(self._message_headers)

    def _build_base_params(self) -> Dict[str, str]:
        params = {
            "VER": str(self._channel_version),
            "zx": self._generate_zx(),
        }
        if self._options.message_url_params:
            params.update({k: str(v) for k, v in self._options.message_url_params.items()})
        if self._http_session_id_param and self._http_session_id_value:
            params[self._http_session_id_param] = self._http_session_id_value
        session_param = self._http_session_id_value or self._session_id_placeholder
        params.setdefault("X-HTTP-Session-Id", session_param)
        params.setdefault("t", str(self._consume_t()))
        return params

    async def _apply_control_headers(self, response: httpx.Response) -> None:
        session_id = response.headers.get("X-HTTP-Session-Id")
        if session_id:
            self._http_session_id_value = session_id

    def _build_url(self, base: str, params: Dict[str, str]) -> str:
        parsed = urlparse(base)
        netloc = parsed.netloc
        if self._host_prefix:
            host, sep, port = netloc.partition(":")
            if not host.startswith(f"{self._host_prefix}."):
                host = f"{self._host_prefix}.{host}"
            netloc = host + (sep + port if sep else "")
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query.update(params)
        encoded_query = urlencode(query, doseq=True)
        return urlunparse(parsed._replace(netloc=netloc, query=encoded_query))

    def _consume_rid(self) -> int:
        rid = self._next_rid
        self._next_rid += 1
        return rid

    @staticmethod
    def _generate_zx() -> str:
        return "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=12))

    @staticmethod
    def _extract_chunks(buffer: str) -> tuple[List[str], str]:
        chunks: List[str] = []
        idx = 0
        while True:
            newline = buffer.find("\n", idx)
            if newline == -1:
                break
            size_str = buffer[idx:newline]
            try:
                size = int(size_str)
            except ValueError:
                return chunks, ""
            start = newline + 1
            end = start + size
            if len(buffer) < end:
                break
            chunks.append(buffer[start:end])
            idx = end
        remainder = buffer[idx:]
        return chunks, remainder

    def _log(self, message: str, **extra: Any) -> None:
        if not self._debug_enabled:
            return
        try:
            payload = json.dumps(extra, default=str)
        except Exception:
            payload = str(extra)
        print(f"[python_webchannel] {message} :: {payload}", flush=True)

    def _sanitize_headers(self, headers: Mapping[str, Any]) -> Dict[str, Any]:
        sanitized: Dict[str, Any] = {}
        for key, value in headers.items():
            key_str = str(key)
            key_lower = key_str.lower()
            if key_lower in {"authorization", "proxy-authorization"}:
                sanitized[key_str] = "***REDACTED***"
            else:
                sanitized[key_str] = value
        return sanitized

    def _apply_persistent_headers(self, headers: Dict[str, Any]) -> None:
        self._refresh_cookie_header()
        if self._cookie_header:
            headers.setdefault("Cookie", self._cookie_header)

    def _refresh_cookie_header(self) -> None:
        jar = self._client.cookies
        items: List[str] = []
        if isinstance(jar, CookieJar):
            for cookie in jar:
                items.append(f"{cookie.name}={cookie.value}")
        else:  # pragma: no cover - fallback for alternate cookie containers
            try:
                items = [f"{key}={value}" for key, value in jar.items()]  # type: ignore[attr-defined]
            except Exception:
                items = []
        if items:
            if self._sid and not any(cookie_str.startswith("SID=") for cookie_str in items):
                items.append(f"SID={self._sid}")
            self._cookie_header = "; ".join(items)
        elif self._cookie_header:
            # Clear cached header if cookies disappeared.
            self._cookie_header = None

    def _apply_fetch_headers(self, headers: Dict[str, str]) -> None:
        for key, value in self._fetch_headers.items():
            headers.setdefault(key, value)

    def _consume_t(self) -> int:
        value = self._next_t
        self._next_t += 1
        return value


__all__ = ["WebChannel", "ChannelState"]
