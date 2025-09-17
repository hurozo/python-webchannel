"""Event infrastructure for python_webchannel."""

from __future__ import annotations

import asyncio
import inspect
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional


EventCallback = Callable[[Any], Any]


class EventType(str, Enum):
    """Canonical WebChannel event identifiers."""

    OPEN = "open"
    CLOSE = "close"
    ERROR = "error"
    MESSAGE = "message"


class EventTarget:
    """Minimal event dispatcher compatible with the Closure EventTarget API."""

    def __init__(self) -> None:
        self._listeners: Dict[Any, List[EventCallback]] = {}
        self._lock = threading.RLock()

    def listen(self, event_type: Any, callback: EventCallback) -> None:
        """Registers a listener for ``event_type``."""

        with self._lock:
            self._listeners.setdefault(event_type, []).append(callback)

    def listen_once(self, event_type: Any, callback: EventCallback) -> None:
        """Registers a listener that will be removed after the first dispatch."""

        def wrapper(payload: Any) -> Any:
            self.unlisten(event_type, wrapper)
            return callback(payload)

        self.listen(event_type, wrapper)

    def unlisten(self, event_type: Any, callback: EventCallback) -> None:
        """Removes a previously registered listener."""

        with self._lock:
            if event_type not in self._listeners:
                return
            listeners = self._listeners[event_type]
            try:
                listeners.remove(callback)
            except ValueError:
                return
            if not listeners:
                del self._listeners[event_type]

    def dispatch_event(self, event_type: Any, payload: Any = None) -> None:
        """Dispatches ``payload`` to all listeners of ``event_type``."""

        with self._lock:
            listeners = list(self._listeners.get(event_type, ()))

        for cb in listeners:
            try:
                result = cb(payload)
            except Exception as exc:  # pragma: no cover - defensive guard
                # We intentionally surface exceptions asynchronously to avoid
                # breaking the dispatcher contract. This mirrors the Closure
                # implementation that defers rethrowing via setTimeout.
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None

                if loop is not None:
                    loop.call_exception_handler(
                        {
                            "message": "Unhandled exception in event listener",
                            "exception": exc,
                        }
                    )
                else:
                    # No loop available; we re-raise to make the failure
                    # visible rather than swallowing it completely.
                    raise
                continue

            if inspect.isawaitable(result):
                async_result = result  # type: ignore[assignment]
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(async_result)  # type: ignore[arg-type]
                except RuntimeError:
                    asyncio.run(async_result)


class MessageEvent:
    """Container for WebChannel message events."""

    def __init__(self, payload: Any) -> None:
        self.headers: Optional[Dict[str, str]] = None
        self.status_code: Optional[int] = None
        self.metadata_key: Optional[str] = None
        self.data: Any = None

        if isinstance(payload, dict):
            headers = payload.pop("__headers__", None)
            status = payload.pop("__status__", None)
            if headers is not None:
                self.headers = headers
            if status is not None:
                self.status_code = status

            metadata = payload.pop("__sm__", None)
            if isinstance(metadata, dict) and metadata:
                self.metadata_key = next(iter(metadata.keys()))
                self.data = metadata[self.metadata_key]
            else:
                self.data = payload
        else:
            self.data = payload


class ErrorEvent(Exception):
    """Represents a WebChannel error notification."""

    def __init__(self, message: str, *, status: str | None = None) -> None:
        super().__init__(message)
        self.status = status


class Event(str, Enum):
    """Internal telemetry events mirrored from requestStats.StatEvent."""

    STAT_EVENT = "stat_event"


class Stat(str, Enum):
    """Simplified subset of requestStats.Stat codes."""

    PROXY = "proxy"
    NOPROXY = "no_proxy"


@dataclass
class StatEvent:
    """Event payload emitted for transport statistics."""

    stat: Stat


_stat_event_target = EventTarget()


def get_stat_event_target() -> EventTarget:
    """Returns the singleton telemetry event target."""

    return _stat_event_target


__all__ = [
    "EventType",
    "EventTarget",
    "MessageEvent",
    "ErrorEvent",
    "Event",
    "Stat",
    "StatEvent",
    "get_stat_event_target",
]
