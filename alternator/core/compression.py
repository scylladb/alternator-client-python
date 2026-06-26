"""Request body compression for bandwidth optimization."""

from __future__ import annotations

import gzip
from collections.abc import Callable
from typing import Any, cast

from botocore.awsrequest import AWSPreparedRequest, AWSRequest


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
