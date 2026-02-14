"""Request body compression for bandwidth optimization."""

from __future__ import annotations

import gzip
from collections.abc import Callable
from typing import Any

from botocore.awsrequest import AWSPreparedRequest


def create_compression_handler(
    min_size: int,
) -> Callable[..., None]:
    """
    Create a botocore event handler for request compression.

    This handler compresses request bodies that exceed the minimum size
    threshold and updates the Content-Encoding header.

    Args:
        min_size: Minimum body size in bytes to trigger compression

    Returns:
        Event handler function for botocore
    """

    def compress_request(request: AWSPreparedRequest, **kwargs: Any) -> None:  # noqa: ANN401 -- botocore event handler signature
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

        compressed = gzip.compress(body_bytes)

        # Only use compression if it actually reduces size
        if len(compressed) >= len(body_bytes):
            return

        # Update request with compressed body
        request.body = compressed
        request.headers["Content-Encoding"] = "gzip"
        request.headers["Content-Length"] = str(len(compressed))

    return compress_request
