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

"""Concurrent, programmable Alternator HTTP/TLS peer for backend contracts."""

from __future__ import annotations

import contextlib
import gzip
import json
import socket
import ssl
import threading
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast
from urllib.parse import quote, urlencode, urlsplit


def _merge_headers(
    defaults: Mapping[str, str], overrides: Mapping[str, str] | None
) -> dict[str, str]:
    """Override HTTP fields case-insensitively, retaining supplied spelling."""
    merged = {name.lower(): (name, value) for name, value in defaults.items()}
    merged.update(
        {name.lower(): (name, value) for name, value in (overrides or {}).items()}
    )
    return dict(merged.values())


@dataclass(frozen=True)
class FakeAlternatorResponse:
    """One response; optional barriers pause after capture and before headers."""

    status: int
    body: bytes
    headers: Mapping[str, str]
    entered: threading.Event | None = None
    release: threading.Event | None = None
    disconnect: bool = False

    @classmethod
    def json(
        cls,
        body: object,
        *,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
        entered: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> FakeAlternatorResponse:
        """Build a DynamoDB JSON response with stable request metadata."""
        return cls(
            status,
            json.dumps(body).encode(),
            _merge_headers(
                {
                    "Content-Type": "application/x-amz-json-1.0",
                    "x-amzn-RequestId": "contract-request",
                },
                headers,
            ),
            entered,
            release,
        )


@dataclass(frozen=True)
class CapturedRequest:
    """Wire bytes and decoded body, retaining original header spelling."""

    method: str
    path: str
    headers: tuple[tuple[str, str], ...]
    body: bytes
    document: dict[str, Any]
    connection_id: int
    operation: str
    table: str | None

    def header(self, name: str) -> str | None:
        """Look up a header without changing the recorded spelling."""
        return next((v for k, v in self.headers if k.lower() == name.lower()), None)


@dataclass
class _Route:
    responses: deque[FakeAlternatorResponse] = field(default_factory=deque)
    fallback: FakeAlternatorResponse | None = None


class FakeAlternatorServer:
    """Thread-safe fake peer. Unscripted POST calls fail visibly with HTTP 400.

    Scripts are consumed in arrival order within an operation/table route.
    Table-specific scripts precede operation-only scripts. GET scripts match
    exact paths, including scoped discovery queries. All waits have finite deadlines.
    """

    def __init__(self) -> None:
        self.requests: list[str] = []
        self._routes: dict[str, _Route] = {}
        self._operations: dict[tuple[str, str | None], _Route] = {}
        self._captured: list[CapturedRequest] = []
        self._condition = threading.Condition()
        self._sockets: set[socket.socket] = set()
        self._next_connection = 0
        self._stopping = threading.Event()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._scheme = "http"

    def start(self, *, ssl_context: ssl.SSLContext | None = None) -> None:
        """Bind an ephemeral loopback port, optionally with verified/mutual TLS."""
        if self._server is not None:
            raise RuntimeError("fake Alternator server is already started")
        self._stopping.clear()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self) -> None:
                super().setup()
                with owner._condition:
                    owner._next_connection += 1
                    self.connection_id = owner._next_connection

            def _dispatch(self) -> None:
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                try:
                    decoded = (
                        gzip.decompress(body)
                        if self.headers.get("Content-Encoding") == "gzip"
                        else body
                    )
                    document = json.loads(decoded) if decoded else {}
                    if not isinstance(document, dict):
                        raise ValueError("expected object")
                except (ValueError, OSError):
                    self.send_error(400, "invalid JSON request")
                    return
                operation = self.headers.get("X-Amz-Target", "").rsplit(".", 1)[-1]
                table = document.get("TableName")
                request = CapturedRequest(
                    self.command,
                    self.path,
                    tuple(self.headers.items()),
                    body,
                    document,
                    self.connection_id,
                    operation,
                    table,
                )
                with owner._condition:
                    owner.requests.append(self.path)
                    owner._captured.append(request)
                    response = owner._response_for(request)
                    owner._condition.notify_all()
                if response.entered is not None:
                    response.entered.set()
                if response.release is not None:
                    # Check shutdown while awaiting release, with a bounded total wait.
                    for _ in range(100):
                        if response.release.wait(0.05) or owner._stopping.is_set():
                            break
                    else:
                        response = FakeAlternatorResponse.json(
                            {
                                "__type": "InternalServerError",
                                "message": "barrier expired",
                            },
                            status=500,
                        )
                if response.disconnect or owner._stopping.is_set():
                    self.close_connection = True
                    return
                try:
                    self.send_response(response.status)
                    for key, value in response.headers.items():
                        self.send_header(key, value)
                    self.send_header("Content-Length", str(len(response.body)))
                    self.end_headers()
                    self.wfile.write(response.body)
                    self.wfile.flush()
                except OSError:
                    self.close_connection = True

            def do_GET(self) -> None:
                self._dispatch()

            def do_POST(self) -> None:
                self._dispatch()

            def log_message(self, fmt: str, *args: object) -> None:
                return

        class Server(ThreadingHTTPServer):
            daemon_threads = False

            def finish_request(
                self, request: socket.socket, client_address: tuple[str, int]
            ) -> None:
                # This runs in a connection worker; accept never performs TLS I/O.
                connection = request
                try:
                    with owner._condition:
                        if owner._stopping.is_set():
                            return
                        request.settimeout(5)
                        owner._sockets.add(request)
                        if ssl_context is not None:
                            connection = ssl_context.wrap_socket(
                                request,
                                server_side=True,
                                do_handshake_on_connect=False,
                            )
                            owner._sockets.discard(request)
                            owner._sockets.add(connection)
                        owner._condition.notify_all()
                    if isinstance(connection, ssl.SSLSocket):
                        try:
                            connection.do_handshake()
                        except OSError:
                            return
                    super().finish_request(connection, client_address)
                finally:
                    with owner._condition:
                        owner._sockets.discard(request)
                        owner._sockets.discard(connection)
                    if connection is not request:
                        connection.close()

        self._server = Server(("127.0.0.1", 0), Handler)
        self._scheme = "https" if ssl_context is not None else "http"
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.02},
            daemon=True,
            name="fake-alternator-server",
        )
        self._thread.start()

    def _response_for(self, request: CapturedRequest) -> FakeAlternatorResponse:
        if request.method == "GET":
            route = self._routes.get(request.path)
        else:
            route = self._operations.get((request.operation, request.table))
            if route is None:
                route = self._operations.get((request.operation, None))
        if route is not None:
            if route.responses:
                return route.responses.popleft()
            if route.fallback is not None:
                return route.fallback
        if request.method == "GET":
            return FakeAlternatorResponse(
                404, b"not found", {"Content-Type": "text/plain"}
            )
        return FakeAlternatorResponse.json(
            {
                "__type": "ValidationException",
                "message": f"unscripted {request.operation} on {request.table}",
            },
            status=400,
        )

    def stop(self) -> None:
        """Interrupt barriers and idle keep-alive sockets, then join listener."""
        self._stopping.set()
        if self._server is not None:
            self._server.shutdown()
            with self._condition:
                sockets = list(self._sockets)
            for connection in sockets:
                with contextlib.suppress(OSError):
                    connection.shutdown(socket.SHUT_RDWR)
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                raise RuntimeError("fake server failed to stop")
        self._server = None
        self._thread = None

    def url(self, path: str = "/") -> str:
        """Build an absolute URL for a path on this server."""
        if self._server is None:
            raise RuntimeError("fake Alternator server is not started")
        host, port = cast(tuple[str, int], self._server.server_address)
        return f"{self._scheme}://{host}:{port}{path}"

    def set_json(
        self,
        path: str,
        body: object,
        *,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Configure a persistent JSON GET response."""
        self.script_get(
            path,
            [],
            fallback=FakeAlternatorResponse.json(body, status=status, headers=headers),
        )

    def set_text(
        self,
        path: str,
        body: str,
        *,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Configure a persistent text GET response."""
        self.script_get(
            path,
            [],
            fallback=FakeAlternatorResponse(
                status,
                body.encode(),
                _merge_headers({"Content-Type": "text/plain"}, headers),
            ),
        )

    def set_localnodes(
        self, nodes: Sequence[str], *, query: str = "", status: int = 200
    ) -> None:
        """Configure Python/native discovery, including an exact scoped query."""
        self.set_json(
            "/localnodes" + (f"?{query}" if query else ""), list(nodes), status=status
        )

    def script_get(
        self,
        path: str,
        responses: Sequence[FakeAlternatorResponse],
        *,
        fallback: FakeAlternatorResponse | None = None,
    ) -> None:
        """Replace a GET FIFO script; exhaustion uses fallback or returns 404."""
        with self._condition:
            self._routes[path] = _Route(deque(responses), fallback)

    def script(
        self,
        operation: str,
        responses: Sequence[FakeAlternatorResponse],
        *,
        table: str | None = None,
        fallback: FakeAlternatorResponse | None = None,
    ) -> None:
        """Replace an operation's FIFO script; exhaustion uses fallback or fails."""
        with self._condition:
            self._operations[(operation, table)] = _Route(deque(responses), fallback)

    def set_operation(
        self,
        operation: str,
        body: object,
        *,
        table: str | None = None,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Configure a repeated operation response."""
        self.script(
            operation,
            [],
            table=table,
            fallback=FakeAlternatorResponse.json(body, status=status, headers=headers),
        )

    def set_topology(
        self,
        local: Sequence[tuple[str, str, str]],
        peers: Sequence[tuple[str, str, str]] = (),
    ) -> None:
        """Serve scoped GET lists from (address, datacenter, rack) rows.

        Unscoped /localnodes returns the first local row's datacenter, as a
        responder does; cluster discovery must union responses from seeds.
        Unknown scopes return 404 unless explicitly scripted (e.g. empty/error
        responses driving a client's fallback chain). This never scripts Scan.
        """
        rows = [*local, *peers]
        scopes: dict[str, list[str]] = {"": []}
        responder_dc = local[0][1] if local else None
        for address, dc, rack in rows:
            queries = [
                urlencode({"dc": dc}, quote_via=quote),
                urlencode({"dc": dc, "rack": rack}, quote_via=quote),
            ]
            if dc == responder_dc:
                queries.append("")
            for query in queries:
                nodes = scopes.setdefault(query, [])
                if address not in nodes:
                    nodes.append(address)
        for query, nodes in scopes.items():
            self.set_localnodes(nodes, query=query)
            # Python quotes spaces as %20; Rust form-encodes them as +. A
            # literal plus remains %2B in both forms. Keep scripted GET paths
            # exact; only this semantic topology helper installs both forms.
            if "%20" in query:
                self.set_localnodes(nodes, query=query.replace("%20", "+"))

    def set_partition_key(self, table: str, name: str, *, key_type: str = "S") -> None:
        """Serve native/Python affinity's DescribeTable HASH-key metadata."""
        self.set_operation(
            "DescribeTable",
            {
                "Table": {
                    "TableName": table,
                    "TableStatus": "ACTIVE",
                    "KeySchema": [{"AttributeName": name, "KeyType": "HASH"}],
                    "AttributeDefinitions": [
                        {"AttributeName": name, "AttributeType": key_type}
                    ],
                }
            },
            table=table,
        )

    def captured_requests(
        self, *, operation: str | None = None, path: str | None = None
    ) -> list[CapturedRequest]:
        """Return a snapshot; discovery can be filtered from data operations."""
        with self._condition:
            return [
                r
                for r in self._captured
                if (operation is None or r.operation == operation)
                and (path is None or r.path == path)
            ]

    def wait_for_connections(self, count: int, *, timeout: float = 3) -> None:
        """Wait until accepted peers are tracked, including pending TLS handshakes."""
        with self._condition:
            if not self._condition.wait_for(
                lambda: len(self._sockets) >= count, timeout
            ):
                raise TimeoutError(f"expected {count} active connections")

    def wait_for_requests(
        self,
        count: int,
        *,
        operation: str | None = None,
        path: str | None = None,
        timeout: float = 3,
    ) -> list[CapturedRequest]:
        """Wait for capture without polling or depending on request completion."""
        with self._condition:
            if not self._condition.wait_for(
                lambda: (
                    len(self.captured_requests(operation=operation, path=path)) >= count
                ),
                timeout,
            ):
                raise TimeoutError(
                    f"expected {count} {operation or path or 'HTTP'} requests"
                )
            return self.captured_requests(operation=operation, path=path)

    def requested_paths(self) -> list[str]:
        """Return captured paths, preserving the original fixture interface."""
        with self._condition:
            return list(self.requests)

    def requested_queries(self) -> list[str]:
        """Return captured discovery query strings."""
        return [urlsplit(path).query for path in self.requested_paths()]
