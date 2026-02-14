"""Request body compression for bandwidth optimization."""

from __future__ import annotations

import gzip
from typing import Any


def create_compression_handler(
    min_size: int,
) -> Any:
    """
    Create a botocore event handler for request compression.

    This handler compresses request bodies that exceed the minimum size
    threshold and updates the Content-Encoding header.

    Args:
        min_size: Minimum body size in bytes to trigger compression

    Returns:
        Event handler function for botocore
    """

    def compress_request(request: Any, **kwargs: Any) -> None:
        """Compress request body if it exceeds threshold."""
        body = request.body
        if body is None:
            return

        # Encode string body once and reuse
        if isinstance(body, str):
            body = body.encode("utf-8")

        if len(body) < min_size:
            return

        compressed = gzip.compress(body)

        # Only use compression if it actually reduces size
        if len(compressed) >= len(body):
            return

        # Update request with compressed body
        request.body = compressed
        request.headers["Content-Encoding"] = "gzip"
        request.headers["Content-Length"] = str(len(compressed))

    return compress_request
