# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Owned HTTP/1 connection leases for synchronous discovery."""

from __future__ import annotations

import contextlib
import http.client
import io
import socket
import threading
import time
import urllib.error
import urllib.request
import urllib.response
from collections import OrderedDict
from collections.abc import Callable
from typing import cast

_PoolKey = tuple[str, str, str | None, str | None]


class _BufferedResponse(urllib.response.addinfourl):
    """Let urllib process status/redirects after the socket has been released."""

    def __init__(
        self, response: http.client.HTTPResponse, body: bytes, url: str
    ) -> None:
        super().__init__(io.BytesIO(body), response.headers, url, response.status)
        self.msg = response.reason


class SyncConnectionPool:
    """Reuse one idle connection per origin and bound the idle cache to 200.

    Active requests have independent leases. Closing cancels their tokens and
    interrupts sockets; a stalled DNS lookup cannot install a connection later.
    Only an explicit reopen admits requests after close, for manager restart.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._idle: OrderedDict[_PoolKey, http.client.HTTPConnection] = OrderedDict()
        self._active: dict[
            http.client.HTTPConnection, tuple[threading.Event, socket.socket | None]
        ] = {}
        self._requests: set[threading.Event] = set()
        self._closed = False

    def begin(self) -> threading.Event:
        cancelled = threading.Event()
        with self._lock:
            if self._closed:
                cancelled.set()
            else:
                self._requests.add(cancelled)
        return cancelled

    def finish(self, cancelled: threading.Event) -> None:
        with self._lock:
            self._requests.discard(cancelled)

    @staticmethod
    def remaining(deadline: float, cancelled: threading.Event) -> float:
        remaining = deadline - time.monotonic()
        if cancelled.is_set():
            raise OSError("discovery request was closed")
        if remaining <= 0:
            raise TimeoutError("discovery request timed out")
        return remaining

    @staticmethod
    def _interrupt(
        connection: http.client.HTTPConnection, stream: socket.socket | None
    ) -> None:
        stream = stream if stream is not None else connection.sock
        if stream is not None:
            with contextlib.suppress(OSError):
                stream.shutdown(socket.SHUT_RDWR)
        connection.close()

    def cancel(self, cancelled: threading.Event) -> None:
        with self._lock:
            cancelled.set()
            active = [
                (connection, stream)
                for connection, (token, stream) in self._active.items()
                if token is cancelled
            ]
        for connection, stream in active:
            self._interrupt(connection, stream)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            for token in self._requests:
                token.set()
            connections: list[
                tuple[http.client.HTTPConnection, socket.socket | None]
            ] = [(connection, None) for connection in self._idle.values()]
            connections.extend(
                (connection, stream) for connection, (_, stream) in self._active.items()
            )
            self._idle.clear()
        for connection, stream in connections:
            self._interrupt(connection, stream)

    def reopen(self) -> None:
        with self._lock:
            self._closed = False

    def _acquire(
        self,
        key: _PoolKey,
        factory: Callable[[float], http.client.HTTPConnection],
        deadline: float,
        cancelled: threading.Event,
    ) -> tuple[http.client.HTTPConnection, bool]:
        with self._lock:
            timeout = self.remaining(deadline, cancelled)
            connection = self._idle.pop(key, None)
            reused = connection is not None
            if connection is None:
                connection = factory(timeout)
            self._active[connection] = (cancelled, connection.sock)
            return connection, reused

    def _release(
        self,
        key: _PoolKey,
        connection: http.client.HTTPConnection,
        cancelled: threading.Event,
        reusable: bool,
    ) -> None:
        discard: list[http.client.HTTPConnection] = []
        with self._lock:
            self._active.pop(connection, None)
            if (
                reusable
                and not self._closed
                and not cancelled.is_set()
                and connection.sock is not None
                and key not in self._idle
            ):
                self._idle[key] = connection
                if len(self._idle) > 200:
                    discard.append(self._idle.popitem(last=False)[1])
            else:
                discard.append(connection)
        for old in discard:
            old.close()

    def open(
        self,
        scheme: str,
        request: urllib.request.Request,
        factory: Callable[[float], http.client.HTTPConnection],
        deadline: float,
        cancelled: threading.Event,
    ) -> http.client.HTTPResponse:
        """Buffer a response, returning a healthy connection before status handling."""
        headers = {name.title(): value for name, value in request.header_items()}
        tunnel_host = cast("str | None", getattr(request, "_tunnel_host", None))
        proxy_auth = headers.get("Proxy-Authorization")
        key = (scheme, request.host, tunnel_host, proxy_auth)

        def create(timeout: float) -> http.client.HTTPConnection:
            connection = factory(timeout)
            if tunnel_host:
                tunnel_headers = (
                    {"Proxy-Authorization": proxy_auth} if proxy_auth else {}
                )
                connection.set_tunnel(tunnel_host, headers=tunnel_headers)
            return connection

        if tunnel_host:
            headers.pop("Proxy-Authorization", None)

        # A server may close an idle socket without announcing it. Retry an
        # idempotent discovery GET once, before receiving complete response headers.
        for attempt in range(2):
            connection, reused = self._acquire(key, create, deadline, cancelled)
            reusable = False
            try:
                connection.timeout = self.remaining(deadline, cancelled)
                if connection.sock is not None:
                    connection.sock.settimeout(connection.timeout)
                try:
                    connection.request(
                        request.get_method(),
                        request.selector,
                        request.data,
                        headers,
                        encode_chunked=request.has_header("Transfer-encoding"),
                    )
                    with self._lock:
                        self._active[connection] = (cancelled, connection.sock)
                    self.remaining(deadline, cancelled)
                    response = connection.getresponse()
                except (OSError, http.client.HTTPException):
                    if reused and attempt == 0 and request.get_method() == "GET":
                        continue
                    raise
                with response:
                    if 200 <= response.status < 300:
                        body = response.read()
                    else:
                        # urllib needs status and headers for errors/redirects,
                        # but discovery never uses their bodies. Fully drain
                        # them without retaining storage proportional to size.
                        buffer = bytearray(64 * 1024)
                        while response.readinto(buffer):
                            self.remaining(deadline, cancelled)
                        body = b""
                self.remaining(deadline, cancelled)
                reusable = response.status >= 200 and not response.will_close
                # urllib only needs the response interface here. Its response
                # processors operate on the buffered addinfourl wrapper.
                return cast(
                    http.client.HTTPResponse,
                    _BufferedResponse(response, body, request.full_url),
                )
            except (OSError, http.client.HTTPException) as error:
                raise urllib.error.URLError(error) from error
            finally:
                self._release(key, connection, cancelled, reusable)
        raise OSError("discovery connection could not be reused")
