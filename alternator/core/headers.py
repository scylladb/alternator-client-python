"""Request header filtering for bandwidth optimization."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from botocore.awsrequest import AWSPreparedRequest

from alternator.config import HeaderWhitelistCallback, HeaderWhitelistContext

if TYPE_CHECKING:
    from alternator.config import Config

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
    config: Config | None = None,
    auth_enabled: bool = False,
    compression_enabled: bool = False,
    custom_whitelist: frozenset[str] | set[str] | None = None,
    whitelist_callback: HeaderWhitelistCallback | None = None,
) -> frozenset[str]:
    """
    Compute the complete header whitelist based on configuration.

    Args:
        config: Client configuration, required when whitelist_callback is set
        auth_enabled: Whether authentication credentials were provided
        compression_enabled: Whether compression is enabled
        custom_whitelist: Additional headers to whitelist
        whitelist_callback: Callback returning additional headers to whitelist

    Returns:
        Frozenset of headers to keep
    """
    result = set(BASE_REQUIRED_HEADERS)
    if auth_enabled:
        result.update(AUTH_HEADERS)

    if compression_enabled:
        result.update(COMPRESSION_HEADERS)

    required_headers = frozenset(result)

    if custom_whitelist:
        result.update(custom_whitelist)

    if whitelist_callback is not None:
        if config is None:
            raise ValueError("config is required when whitelist_callback is set")
        context = HeaderWhitelistContext(
            config=config,
            auth_enabled=auth_enabled,
            compression_enabled=compression_enabled,
            required_headers=required_headers,
        )
        result.update(whitelist_callback(context))

    return frozenset(result)


def create_header_filter_handler(
    whitelist: frozenset[str],
) -> Callable[..., None]:
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

    def filter_headers(request: AWSPreparedRequest, **kwargs: Any) -> None:  # noqa: ANN401 -- botocore event handler signature
        """Filter request headers to only include whitelisted ones."""
        if not hasattr(request, "headers"):
            return

        headers_to_remove = [
            key for key in request.headers if key.lower() not in whitelist_lower
        ]

        for key in headers_to_remove:
            del request.headers[key]

    return filter_headers
