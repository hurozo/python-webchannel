"""Utility helpers mirroring goog.net.rpc.HttpCors."""

from __future__ import annotations

from typing import Mapping
from urllib.parse import quote


HTTP_HEADERS_PARAM_NAME = "$httpHeaders"
HTTP_METHOD_PARAM_NAME = "$httpMethod"


def generate_http_headers_overwrite_param(headers: Mapping[str, str]) -> str:
    return "\r\n".join(f"{key}:{value}" for key, value in headers.items()) + "\r\n"


def generate_encoded_http_headers_overwrite_param(headers: Mapping[str, str]) -> str:
    return quote(generate_http_headers_overwrite_param(headers))


__all__ = [
    "HTTP_HEADERS_PARAM_NAME",
    "HTTP_METHOD_PARAM_NAME",
    "generate_http_headers_overwrite_param",
    "generate_encoded_http_headers_overwrite_param",
]
