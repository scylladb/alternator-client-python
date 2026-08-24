# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""HTTP client helpers for node discovery."""

from __future__ import annotations

import asyncio
import http.client
import ipaddress
import json
import logging
import socket
import ssl
import sys
import threading
import time
import urllib.request
from collections.abc import Awaitable, Sequence
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable
from urllib.parse import urlsplit

if TYPE_CHECKING:
    import aiohttp

    from alternator.config import TLS

logger = logging.getLogger("alternator")
_sync_request_state = threading.local()

# Alternator's /localnodes endpoint never emits redirects. Redirect responses are
# therefore outside the discovery fallback contract, and both transports retain
# their standard redirect handling.


def _parse_localnodes(data: str) -> list[str]:
    """Parse a non-empty /localnodes response containing only IP literals."""
    nodes = json.loads(data)
    if not isinstance(nodes, list) or not nodes:
        return []

    parsed: list[str] = []
    for node in nodes:
        if not isinstance(node, str) or not node:
            return []
        try:
            ipaddress.ip_address(node)
        except ValueError:
            return []
        parsed.append(node)
    return parsed


def _create_connection_with_deadline(
    address: tuple[str, int],
    deadline: float,
    source_address: tuple[str, int] | None = None,
) -> socket.socket:
    """Connect to one resolved address within a shared request deadline."""
    host, port = address
    addresses = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    last_error: OSError | None = None

    for index, (family, socktype, proto, _, sockaddr) in enumerate(addresses):
        connection: socket.socket | None = None
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("discovery request timed out")

            # A socket timeout applies independently to every address. Divide
            # the remaining budget so an unresponsive candidate cannot consume
            # the time needed to try all later DNS answers.
            candidates_left = len(addresses) - index
            connection = socket.socket(family, socktype, proto)
            connection.settimeout(remaining / candidates_left)
            if source_address is not None:
                connection.bind(source_address)
            connection.connect(sockaddr)

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("discovery request timed out")
            connection.settimeout(remaining)
            return connection
        except OSError as error:
            last_error = error
            if connection is not None:
                connection.close()

    if last_error is not None:
        raise last_error
    raise OSError("getaddrinfo returns an empty list")


class _DeadlineHTTPConnection(http.client.HTTPConnection):
    """HTTP connection that apportions one deadline across DNS addresses."""

    def connect(self) -> None:
        deadline = cast(float, _sync_request_state.__dict__["deadline"])
        sys.audit("http.client.connect", self, self.host, self.port)
        self.sock = _create_connection_with_deadline(
            (self.host, self.port),
            deadline,
            self.source_address,  # type: ignore[attr-defined]
        )
        if self._tunnel_host:  # type: ignore[attr-defined]
            self._tunnel()  # type: ignore[attr-defined]


class _DeadlineHTTPSConnection(http.client.HTTPSConnection):
    """HTTPS connection that apportions one deadline across DNS addresses."""

    def connect(self) -> None:
        deadline = cast(float, _sync_request_state.__dict__["deadline"])
        server_hostname = self._tunnel_host or self.host  # type: ignore[attr-defined]
        sys.audit("http.client.connect", self, self.host, self.port)
        self.sock = _create_connection_with_deadline(
            (self.host, self.port),
            deadline,
            self.source_address,  # type: ignore[attr-defined]
        )
        if self._tunnel_host:  # type: ignore[attr-defined]
            self._tunnel()  # type: ignore[attr-defined]
        context = cast(ssl.SSLContext, self.__dict__["_context"])
        self.sock = context.wrap_socket(self.sock, server_hostname=server_hostname)


class _DeadlineHTTPHandler(urllib.request.HTTPHandler):
    """urllib HTTP handler whose connections share one request deadline."""

    def http_open(self, request: urllib.request.Request) -> http.client.HTTPResponse:
        return self.do_open(_DeadlineHTTPConnection, request)


class _DeadlineHTTPSHandler(urllib.request.HTTPSHandler):
    """urllib HTTPS handler whose connections share one request deadline."""

    def __init__(
        self,
        ssl_context: ssl.SSLContext | None,
    ) -> None:
        super().__init__(context=ssl_context)
        self._ssl_context = ssl_context

    def https_open(self, request: urllib.request.Request) -> http.client.HTTPResponse:
        return self.do_open(
            _DeadlineHTTPSConnection,
            request,
            context=self._ssl_context,
        )


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

    timed_out_workers: dict[tuple[str, str | None, int | None], set[object]] = {}
    active_origins_lock = threading.Lock()

    def fetch_nodes_unbounded(url: str, deadline: float) -> Sequence[str]:
        """Fetch nodes while the caller enforces the total timeout."""
        try:
            request = urllib.request.Request(url)
            request.add_header("Accept", "application/json")
            opener = urllib.request.build_opener(
                _DeadlineHTTPHandler(),
                _DeadlineHTTPSHandler(ssl_context),
            )

            _sync_request_state.deadline = deadline
            try:
                with opener.open(request, timeout=timeout_seconds) as response:
                    data = response.read().decode("utf-8")
                    return _parse_localnodes(data)
            finally:
                del _sync_request_state.deadline
        except (urllib.error.URLError, json.JSONDecodeError, OSError, ValueError) as e:
            logger.debug(
                "Failed to fetch nodes from %s: %s",
                url,
                e,
                extra={"event": "sync_node_fetch_failed", "url": url, "error": str(e)},
            )
            return []

    def fetch_nodes(url: str) -> Sequence[str]:
        """Fetch node list with a timeout that also bounds DNS resolution."""
        try:
            parsed = urlsplit(url)
            origin = (parsed.scheme, parsed.hostname, parsed.port)
        except ValueError as e:
            logger.debug(
                "Failed to parse discovery URL %s: %s",
                url,
                e,
                extra={"event": "sync_node_fetch_failed", "url": url, "error": str(e)},
            )
            return []

        with active_origins_lock:
            if timed_out_workers.get(origin):
                logger.debug(
                    "A timed-out discovery request for %s is still running",
                    url,
                    extra={"event": "sync_node_fetch_still_running", "url": url},
                )
                return []

        done = threading.Event()
        result: list[Sequence[str]] = []
        worker_token = object()
        deadline = time.monotonic() + timeout_seconds

        def run_fetch() -> None:
            try:
                result.append(fetch_nodes_unbounded(url, deadline))
            except Exception as e:
                logger.debug(
                    "Failed to fetch nodes from %s: %s",
                    url,
                    e,
                    extra={
                        "event": "sync_node_fetch_failed",
                        "url": url,
                        "error": str(e),
                    },
                )
                result.append([])
            finally:
                with active_origins_lock:
                    done.set()
                    workers = timed_out_workers.get(origin)
                    if workers is not None:
                        workers.discard(worker_token)
                        if not workers:
                            timed_out_workers.pop(origin, None)

        worker = threading.Thread(
            target=run_fetch,
            daemon=True,
            name="alternator-node-fetch",
        )
        try:
            worker.start()
        except RuntimeError as e:
            logger.debug(
                "Failed to start discovery request for %s: %s",
                url,
                e,
                extra={"event": "sync_node_fetch_failed", "url": url, "error": str(e)},
            )
            return []

        if not done.wait(timeout=max(0.0, deadline - time.monotonic())):
            with active_origins_lock:
                if done.is_set():
                    return result[0] if result else []
                timed_out_workers.setdefault(origin, set()).add(worker_token)
            logger.debug(
                "Timed out fetching nodes from %s",
                url,
                extra={"event": "sync_node_fetch_timeout", "url": url},
            )
            return []
        return result[0] if result else []

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
                    ssl=self._ssl_context if self._ssl_context else False,
                    # Re-resolve on every new connection while coalescing an
                    # in-flight lookup that outlives a request timeout.
                    ttl_dns_cache=0,
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
                if response.status < 200 or response.status >= 300:
                    logger.debug(
                        "Failed to fetch nodes from %s: HTTP %d",
                        url,
                        response.status,
                        extra={
                            "event": "async_node_fetch_failed",
                            "url": url,
                            "status": response.status,
                        },
                    )
                    return []
                return _parse_localnodes(data)
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
