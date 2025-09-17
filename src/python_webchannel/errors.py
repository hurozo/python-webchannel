"""Error primitives for the python_webchannel transport."""

from __future__ import annotations

from enum import Enum
from typing import Optional


class ErrorCode(int, Enum):
    """Subset of goog.net.ErrorCode used by WebChannel callers."""

    NO_ERROR = 0
    ACCESS_DENIED = 1
    FILE_NOT_FOUND = 2
    CUSTOM_ERROR = 3
    HTTP_ERROR = 4
    ABORT = 5
    TIMEOUT = 6
    OFFLINE = 7
    FAILURE = 8


class WebChannelError(Exception):
    """Structured WebChannel error returned by the backend."""

    def __init__(self, status: Optional[str], message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message

    def to_dict(self) -> dict[str, str | None]:
        return {"status": self.status, "message": self.message}


__all__ = ["ErrorCode", "WebChannelError"]
