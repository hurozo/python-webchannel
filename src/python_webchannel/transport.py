"""Factory for creating WebChannel instances."""

from __future__ import annotations

from typing import Optional

from .channel import WebChannel
from .options import WebChannelOptions


class WebChannelTransport:
    """Python port of goog.net.WebChannelTransport."""

    CLIENT_VERSION = 22

    def __init__(self) -> None:
        pass

    def create_web_channel(
        self, url: str, options: Optional[WebChannelOptions] = None
    ) -> WebChannel:
        return WebChannel(url, options or WebChannelOptions())


def create_web_channel_transport() -> WebChannelTransport:
    return WebChannelTransport()


__all__ = ["WebChannelTransport", "create_web_channel_transport"]
