"""HTTP client helpers for node discovery."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import urllib.request
from collections.abc import Awaitable, Sequence
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import aiohttp

    from alternator.config import TLS

logger = logging.getLogger("alternator")


@runtime_checkable
class SyncHttpFetcher(Protocol):
    """Protocol for synchronous HTTP fetching of node lists."""

    def __call__(self, url: str) -> Sequence[str]:
        """
        Fetch node list from a URL.

        Args:
            url: The URL to fetch nodes from

        Returns:
            Sequence of node addresses (empty on error)
        """
        ...


@runtime_checkable
class AsyncHttpFetcher(Protocol):
    """Protocol for asynchronous HTTP fetching of node lists."""

    def __call__(self, url: str) -> Awaitable[Sequence[str]]:
        """
        Fetch node list from a URL asynchronously.

        Args:
            url: The URL to fetch nodes from

        Returns:
            Awaitable that resolves to sequence of node addresses (empty on error)
        """
        ...


def create_sync_http_fetcher(
    ssl_context: ssl.SSLContext | None = None,
    timeout_seconds: float = 5.0,
) -> SyncHttpFetcher:
    """
    Create a synchronous HTTP fetcher for /localnodes endpoint.

    Args:
        ssl_context: SSL context for HTTPS connections
        timeout_seconds: Request timeout in seconds

    Returns:
        Callable that fetches node list from URL
    """

    def fetch_nodes(url: str) -> Sequence[str]:
        """Fetch node list from /localnodes endpoint."""
        try:
            request = urllib.request.Request(url)
            request.add_header("Accept", "application/json")

            with urllib.request.urlopen(
                request, context=ssl_context, timeout=timeout_seconds
            ) as response:
                data = response.read().decode("utf-8")
                nodes = json.loads(data)
                if isinstance(nodes, list):
                    return [str(node) for node in nodes]
                return []
        except (urllib.error.URLError, json.JSONDecodeError, OSError, ValueError) as e:
            logger.debug(
                "Failed to fetch nodes from %s: %s",
                url,
                e,
                extra={"event": "sync_node_fetch_failed", "url": url, "error": str(e)},
            )
            return []

    return fetch_nodes


def create_ssl_context(tls_config: TLS) -> ssl.SSLContext:
    """
    Create an SSL context from TLS configuration.

    Args:
        tls_config: TLS configuration settings

    Returns:
        Configured SSL context
    """
    from alternator.core.tls import create_ssl_context as _create_ssl_context

    return _create_ssl_context(tls_config)


class AsyncNodeFetcher:
    """Async HTTP fetcher for /localnodes endpoint with session management."""

    def __init__(
        self,
        ssl_context: ssl.SSLContext | None = None,
        timeout_seconds: float = 5.0,
    ) -> None:
        try:
            import aiohttp as _aiohttp
        except ImportError as e:
            raise ImportError(
                "aiohttp is required for async support. "
                "Install with: pip install alternator[async]"
            ) from e
        self._aiohttp = _aiohttp
        self._ssl_context = ssl_context
        self._timeout_seconds = timeout_seconds
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure a valid session exists, creating one if needed."""
        aiohttp = self._aiohttp
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._timeout_seconds)
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    ssl=self._ssl_context if self._ssl_context else False
                ),
                timeout=timeout,
            )
        return self._session

    async def __call__(self, url: str) -> Sequence[str]:
        """Fetch node list from /localnodes endpoint asynchronously."""
        aiohttp = self._aiohttp
        try:
            sess = await self._ensure_session()
            async with sess.get(
                url, headers={"Accept": "application/json"}
            ) as response:
                data = await response.text()
                nodes = json.loads(data)
                if isinstance(nodes, list):
                    return [str(node) for node in nodes]
                return []
        except (
            aiohttp.ClientError,
            json.JSONDecodeError,
            asyncio.TimeoutError,
            OSError,
            ValueError,
        ) as e:
            logger.debug(
                "Failed to fetch nodes from %s: %s",
                url,
                e,
                extra={"event": "async_node_fetch_failed", "url": url, "error": str(e)},
            )
            return []

    async def close(self) -> None:
        """Close the underlying aiohttp session."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None


def create_async_http_fetcher(
    ssl_context: ssl.SSLContext | None = None,
    timeout_seconds: float = 5.0,
) -> AsyncNodeFetcher:
    """
    Create an asynchronous HTTP fetcher for /localnodes endpoint.

    Args:
        ssl_context: SSL context for HTTPS connections
        timeout_seconds: Request timeout in seconds

    Returns:
        Async callable that fetches node list from URL

    Note:
        Requires aiohttp to be installed (part of [async] extras).
    """
    return AsyncNodeFetcher(ssl_context, timeout_seconds)
