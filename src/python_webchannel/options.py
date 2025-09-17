"""Option structures for configuring WebChannel instances."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Mapping, MutableMapping, Optional


@dataclass(slots=True)
class WebChannelOptions:
    """Python representation of WebChannel configuration knobs."""

    message_url_params: Optional[MutableMapping[str, str]] = field(default=None)
    message_headers: Optional[MutableMapping[str, str]] = field(default=None)
    init_message_headers: Optional[MutableMapping[str, str]] = field(default=None)
    internal_channel_params: Optional[Mapping[str, object]] = field(default=None)
    http_session_id_param: Optional[str] = field(default=None)
    message_content_type: Optional[str] = field(default=None)
    client_profile: Optional[str] = field(default=None)
    encode_init_message_headers: bool = field(default=False)
    supports_cross_domain_xhr: bool = field(default=False)
    send_raw_json: bool = field(default=False)
    force_long_polling: bool = field(default=False)
    detect_buffering_proxy: bool = field(default=False)
    long_polling_timeout: Optional[int] = field(default=None)
    use_fetch_streams: bool = field(default=False)
    http_headers_overwrite_param: Optional[str] = field(default=None)
    client_protocol_header_required: bool = field(default=False)
    fetch_headers: Optional[MutableMapping[str, str]] = field(default=None)

    def copy(self) -> "WebChannelOptions":
        return WebChannelOptions(
            message_url_params=dict(self.message_url_params or {}),
            message_headers=dict(self.message_headers or {}),
            init_message_headers=dict(self.init_message_headers or {}),
            internal_channel_params=dict(self.internal_channel_params or {}),
            http_session_id_param=self.http_session_id_param,
            message_content_type=self.message_content_type,
            client_profile=self.client_profile,
            encode_init_message_headers=self.encode_init_message_headers,
            supports_cross_domain_xhr=self.supports_cross_domain_xhr,
            send_raw_json=self.send_raw_json,
            force_long_polling=self.force_long_polling,
            detect_buffering_proxy=self.detect_buffering_proxy,
            long_polling_timeout=self.long_polling_timeout,
            use_fetch_streams=self.use_fetch_streams,
            http_headers_overwrite_param=self.http_headers_overwrite_param,
            client_protocol_header_required=
            self.client_protocol_header_required,
            fetch_headers=dict(self.fetch_headers or {}),
        )


__all__ = ["WebChannelOptions"]
