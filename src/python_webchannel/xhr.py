"""Minimal XhrIo implementation backed by httpx."""

from __future__ import annotations

import asyncio
from enum import Enum
from typing import Any, Dict, Optional

import httpx

from .errors import ErrorCode
from .events import EventTarget


class XhrEventType(str, Enum):
    COMPLETE = "complete"


class XhrIo(EventTarget):
    """Asynchronous HTTP helper that mirrors goog.net.XhrIo."""

    def __init__(self, client: Optional[httpx.AsyncClient] = None) -> None:
        super().__init__()
        self._client = client or httpx.AsyncClient()
        self._with_credentials = False
        self._last_error_code: ErrorCode = ErrorCode.NO_ERROR
        self._last_error: Optional[str] = None
        self._response: Optional[httpx.Response] = None

    async def close(self) -> None:
        await self._client.aclose()

    def setWithCredentials(self, value: bool) -> None:  # Closure-style alias
        self.set_with_credentials(value)

    def set_with_credentials(self, value: bool) -> None:
        self._with_credentials = value

    def send(
        self,
        url: str,
        method: str = "GET",
        content: Optional[str | bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout_secs: Optional[int] = None,
    ) -> None:
        asyncio.create_task(
            self._dispatch_request(url, method, content, headers, timeout_secs)
        )

    async def _dispatch_request(
        self,
        url: str,
        method: str,
        content: Optional[str | bytes],
        headers: Optional[Dict[str, str]],
        timeout_secs: Optional[int],
    ) -> None:
        try:
            response = await self._client.request(
                method,
                url,
                content=content,
                headers=headers,
                timeout=timeout_secs,
            )
            self._response = response
            if response.status_code >= 400:
                self._last_error_code = ErrorCode.HTTP_ERROR
                self._last_error = response.text
            else:
                self._last_error_code = ErrorCode.NO_ERROR
                self._last_error = None
        except httpx.TimeoutException as exc:  # pragma: no cover - network error
            self._response = None
            self._last_error_code = ErrorCode.TIMEOUT
            self._last_error = str(exc)
        except httpx.HTTPStatusError as exc:  # pragma: no cover - network error
            self._response = exc.response
            self._last_error_code = ErrorCode.HTTP_ERROR
            self._last_error = str(exc)
        except httpx.HTTPError as exc:  # pragma: no cover - network error
            self._response = None
            self._last_error_code = ErrorCode.FAILURE
            self._last_error = str(exc)
        finally:
            self.dispatch_event(XhrEventType.COMPLETE, None)

    # Accessors mirroring goog.net.XhrIo
    def getLastErrorCode(self) -> ErrorCode:
        return self._last_error_code

    def getLastError(self) -> Optional[str]:
        return self._last_error

    def getStatus(self) -> int:
        if self._response is None:
            return 0
        return self._response.status_code

    def getResponseText(self) -> Optional[str]:
        if self._response is None:
            return None
        return self._response.text

    def getResponseJson(self) -> Any:
        if self._response is None:
            return None
        try:
            return self._response.json()
        except ValueError:
            return None


__all__ = ["XhrIo", "XhrEventType"]
