"""Request body compression for bandwidth optimization."""

from __future__ import annotations

import contextlib
import gzip
import zlib
from collections.abc import Callable, Sequence
from typing import Any, cast

from botocore.awsrequest import AWSPreparedRequest, AWSRequest

from alternator.config import ResponseCompression


def create_compression_handler(
    min_size: int,
    gzip_level: int = 9,
) -> Callable[..., None]:
    """
    Create a botocore event handler for request compression.

    This handler compresses request bodies that exceed the minimum size
    threshold and updates the Content-Encoding header.

    Args:
        min_size: Minimum body size in bytes to trigger compression
        gzip_level: gzip compression level, from 0 through 9

    Returns:
        Event handler function for botocore
    """

    def compress_request(
        request: AWSPreparedRequest | AWSRequest,
        **kwargs: Any,  # noqa: ANN401 -- botocore event handler signature
    ) -> None:
        """Compress request body if it exceeds threshold."""
        raw_body = request.body
        if raw_body is None:
            return

        # Normalize body to bytes
        if isinstance(raw_body, str):
            body_bytes = raw_body.encode("utf-8")
        elif isinstance(raw_body, (bytes, bytearray)):
            body_bytes = bytes(raw_body)
        else:
            return

        if len(body_bytes) < min_size:
            return

        compressed = gzip.compress(body_bytes, compresslevel=gzip_level)

        # Only use compression if it actually reduces size
        if len(compressed) >= len(body_bytes):
            return

        # Update request with compressed body
        _set_request_body(request, compressed)
        request.headers["Content-Encoding"] = "gzip"
        request.headers["Content-Length"] = str(len(compressed))

    return compress_request


def _set_request_body(request: AWSPreparedRequest | AWSRequest, body: bytes) -> None:
    """Set request body for both prepared and pre-signing request objects."""
    mutable_request = cast(Any, request)
    if isinstance(request, AWSPreparedRequest):
        mutable_request.body = body
        return
    if isinstance(request, AWSRequest):
        mutable_request.data = body
        return
    mutable_request.body = body


def create_response_compression_request_handler(
    encodings: Sequence[ResponseCompression],
) -> Callable[..., None]:
    """Create a handler that requests compressed HTTP responses."""
    accept_encoding = _response_accept_encoding(encodings)

    def request_response_compression(
        request: AWSPreparedRequest | AWSRequest,
        **kwargs: Any,  # noqa: ANN401 -- botocore event handler signature
    ) -> None:
        if not accept_encoding:
            return
        headers = getattr(request, "headers", None)
        if headers is None:
            return

        current = _get_header(headers, "Accept-Encoding")
        if (
            current is None
            or current.strip() == ""
            or current.strip().lower() == "identity"
        ):
            headers["Accept-Encoding"] = accept_encoding

    return request_response_compression


def create_response_compression_decode_handler() -> Callable[..., None]:
    """Create a handler that decodes compressed HTTP responses before parsing."""

    def decode_response_compression(
        response_dict: dict[str, Any],
        **kwargs: Any,  # noqa: ANN401 -- botocore event handler signature
    ) -> None:
        headers = response_dict.get("headers")
        encoding = _get_header(headers, "Content-Encoding")
        if encoding is None:
            return

        normalized_encoding = encoding.strip().lower()
        if normalized_encoding == ResponseCompression.GZIP.value:
            response_dict["body"] = _decode_gzip_response(response_dict.get("body"))
        elif normalized_encoding == ResponseCompression.DEFLATE.value:
            response_dict["body"] = _decode_deflate_response(response_dict.get("body"))
        else:
            return

        _delete_header(headers, "Content-Encoding")
        _delete_header(headers, "Content-Length")

    return decode_response_compression


def _response_accept_encoding(
    encodings: Sequence[ResponseCompression],
) -> str:
    seen: set[ResponseCompression] = set()
    parts: list[str] = []
    for encoding in encodings:
        if encoding in seen:
            continue
        seen.add(encoding)
        parts.append(encoding.value)
    return ", ".join(parts)


def _decode_gzip_response(body: object) -> bytes:
    try:
        return gzip.decompress(_response_body_as_bytes(body))
    except Exception as exc:
        raise ValueError(f"decode gzip response: {exc}") from exc


def _decode_deflate_response(body: object) -> bytes:
    try:
        return zlib.decompress(_response_body_as_bytes(body))
    except Exception as exc:
        raise ValueError(f"decode deflate response: {exc}") from exc


def _response_body_as_bytes(body: object) -> bytes:
    if isinstance(body, bytes):
        return body
    if isinstance(body, str):
        return body.encode("utf-8")
    if isinstance(body, bytearray | memoryview):
        return bytes(body)
    raise TypeError(f"unsupported response body type {type(body).__name__}")


def _get_header(headers: object, name: str) -> str | None:
    if headers is None:
        return None

    getter = getattr(headers, "get", None)
    if callable(getter):
        for key in (name, name.lower(), name.title()):
            value = getter(key)
            if value is not None:
                return _header_value_to_str(value)

    for header_key, value in _header_items(headers):
        if _header_value_to_str(header_key).lower() == name.lower():
            return _header_value_to_str(value)
    return None


def _delete_header(headers: object, name: str) -> None:
    if headers is None:
        return

    mutable_headers = cast(Any, headers)
    for key in (name, name.lower(), name.title()):
        with contextlib.suppress(KeyError, TypeError, AttributeError):
            del mutable_headers[key]

    for header_key, _ in _header_items(headers):
        if _header_value_to_str(header_key).lower() == name.lower():
            with contextlib.suppress(KeyError, TypeError, AttributeError):
                del mutable_headers[header_key]


def _header_items(headers: object) -> list[tuple[object, object]]:
    try:
        return list(dict(cast(Any, headers)).items())
    except (TypeError, ValueError):
        return []


def _header_value_to_str(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)
