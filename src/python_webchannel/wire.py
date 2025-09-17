"""V8 wire-format helpers used by the WebChannel transport."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable, Iterable, List, MutableSequence, Sequence


LATEST_CHANNEL_VERSION = 8
RAW_DATA_KEY = "__data__"


@dataclass(slots=True)
class QueuedMap:
    map_id: int
    payload: dict
    context: dict | None = None

    def raw_data_size(self) -> int | None:
        raw = self.payload.get(RAW_DATA_KEY)
        if isinstance(raw, str):
            return len(raw)
        return None


class WireV8:
    """Implementation of the v8 wire-format codec."""

    def encode_message_queue(
        self,
        queue: Sequence[QueuedMap],
        count: int,
        bad_map_handler: Callable[[dict], None] | None = None,
    ) -> str:
        if count == 0:
            return "count=0&ofs=0"

        offset = -1
        while True:
            parts: List[str] = [f"count={count}"]
            if offset == -1:
                offset = queue[0].map_id if count > 0 else 0
            parts.append(f"ofs={offset}")

            done = True
            for i in range(count):
                entry = queue[i]
                map_id = entry.map_id - offset
                if map_id < 0:
                    offset = max(0, entry.map_id - 100)
                    done = False
                    break
                try:
                    self._encode_message(entry.payload, parts, f"req{map_id}_")
                except Exception:  # pragma: no cover - parity with Closure impl
                    if bad_map_handler:
                        bad_map_handler(entry.payload)
            if done:
                return "&".join(parts)

    def _encode_message(
        self, payload: dict, buffer: MutableSequence[str], prefix: str
    ) -> None:
        for key, value in payload.items():
            encoded_value = value
            if isinstance(value, (dict, list)):
                encoded_value = json.dumps(value, separators=(",", ":"))
            buffer.append(f"{prefix}{key}={self._uri_encode(encoded_value)}")

    def decode_message(self, message_text: str):
        result = json.loads(message_text)
        if not isinstance(result, list):
            raise ValueError("Decoded payload must be a JSON array")
        return result

    @staticmethod
    def _uri_encode(value) -> str:
        from urllib.parse import quote

        return quote(str(value), safe="~()*!.'")


__all__ = ["WireV8", "QueuedMap", "LATEST_CHANNEL_VERSION", "RAW_DATA_KEY"]
