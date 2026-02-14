"""Request header filtering for bandwidth optimization."""

from __future__ import annotations

from typing import Any

# Base required headers that are always needed
BASE_REQUIRED_HEADERS = frozenset(
    {
        "Host",
        "X-Amz-Target",
        "Content-Type",
        "Content-Length",
        "Accept-Encoding",
    }
)

# Authentication headers (added when auth is enabled)
AUTH_HEADERS = frozenset(
    {
        "Authorization",
        "X-Amz-Date",
        "X-Amz-Security-Token",  # For temporary credentials
    }
)

# Compression header (added when compression is enabled)
COMPRESSION_HEADERS = frozenset(
    {
        "Content-Encoding",
    }
)


def compute_header_whitelist(
    *,
    auth_enabled: bool = True,
    compression_enabled: bool = False,
    custom_whitelist: frozenset[str] | set[str] | None = None,
) -> frozenset[str]:
    """
    Compute the complete header whitelist based on configuration.

    Args:
        auth_enabled: Whether authentication is enabled
        compression_enabled: Whether compression is enabled
        custom_whitelist: Additional headers to whitelist

    Returns:
        Frozenset of headers to keep
    """
    result = set(BASE_REQUIRED_HEADERS)

    if auth_enabled:
        result.update(AUTH_HEADERS)

    if compression_enabled:
        result.update(COMPRESSION_HEADERS)

    if custom_whitelist:
        result.update(custom_whitelist)

    return frozenset(result)


def create_header_filter_handler(
    whitelist: frozenset[str],
) -> Any:
    """
    Create a botocore event handler for header filtering.

    This handler removes headers not in the whitelist to reduce
    request size.

    Args:
        whitelist: Set of header names to keep (case-insensitive matching)

    Returns:
        Event handler function for botocore
    """
    # Create case-insensitive lookup
    whitelist_lower = frozenset(h.lower() for h in whitelist)

    def filter_headers(request: Any, **kwargs: Any) -> None:
        """Filter request headers to only include whitelisted ones."""
        if not hasattr(request, "headers"):
            return

        headers_to_remove = [
            key for key in request.headers if key.lower() not in whitelist_lower
        ]

        for key in headers_to_remove:
            del request.headers[key]

    return filter_headers
