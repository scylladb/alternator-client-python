# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Wire-level reuse and cleanup for synchronous discovery connections."""

from __future__ import annotations

import http.client
import io
import socket
import ssl
import threading
import time
from email.message import Message
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest.mock import MagicMock, Mock
from urllib.error import URLError
from urllib.request import Request

import pytest

from alternator._http import create_sync_http_fetcher
from alternator._sync_http_pool import SyncConnectionPool
from tests.fake_server import FakeAlternatorResponse, FakeAlternatorServer


def assert_peer_closed(server: FakeAlternatorServer) -> None:
    """Observe EOF at the server, including sockets idle between refreshes."""
    with server._condition:
        assert server._condition.wait_for(lambda: not server._sockets, timeout=2)


@pytest.mark.parametrize("stage", ["cancel", "construct", "start", "start_other"])
def test_fetch_worker_prestart_failure_releases_only_its_request(
    monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    fetcher = create_sync_http_fetcher(timeout_seconds=1)
    pool = fetcher._pool
    outstanding = pool.begin()
    if stage == "cancel":
        begin = pool.begin

        def begin_then_close() -> threading.Event:
            token = begin()
            pool.close()
            return token

        monkeypatch.setattr(pool, "begin", begin_then_close)
    elif stage == "construct":
        monkeypatch.setattr(
            threading, "Thread", Mock(side_effect=RuntimeError("failed"))
        )
    else:
        error = (
            ValueError("failed") if stage == "start_other" else RuntimeError("failed")
        )
        monkeypatch.setattr(threading.Thread, "start", Mock(side_effect=error))
    try:
        if stage in {"construct", "start_other"}:
            error_type = RuntimeError if stage == "construct" else ValueError
            with pytest.raises(error_type, match="failed"):
                fetcher("http://127.0.0.1:8000/localnodes")
        else:
            assert list(fetcher("http://127.0.0.1:8000/localnodes")) == []
        assert pool._requests == {outstanding}
        assert not pool._active
    finally:
        pool.finish(outstanding)
        fetcher.close()


def test_interrupted_worker_start_cancels_late_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = threading.Event(), threading.Event()
    workers: list[threading.Thread] = []
    start = threading.Thread.start

    def interrupted_start(worker: threading.Thread) -> None:
        start(worker)
        workers.append(worker)
        assert entered.wait(timeout=1)
        raise KeyboardInterrupt("start interrupted")

    def resolve(host: object, port: object, *args: object) -> list[Any]:
        entered.set()
        assert release.wait(timeout=2)
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 8000))]

    connect = Mock(side_effect=AssertionError("cancelled worker connected"))
    monkeypatch.setattr(threading.Thread, "start", interrupted_start)
    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    monkeypatch.setattr(socket.socket, "connect", connect)
    fetcher = create_sync_http_fetcher(timeout_seconds=1)
    outstanding = fetcher._pool.begin()
    try:
        with pytest.raises(KeyboardInterrupt, match="start interrupted"):
            fetcher("http://127.0.0.1:8000/localnodes")
        assert fetcher._pool._requests == {outstanding}
        release.set()
        workers[0].join(timeout=1)
        assert not workers[0].is_alive()
        connect.assert_not_called()
    finally:
        release.set()
        for worker in workers:
            worker.join(timeout=2)
        fetcher._pool.finish(outstanding)
        fetcher.close()


@pytest.mark.parametrize("statuses", [(200, 200, 200), (500, 503, 200)])
def test_sync_discovery_drains_and_reuses_responses(
    fake_alternator_server: FakeAlternatorServer, statuses: tuple[int, ...]
) -> None:
    server = fake_alternator_server
    server.script_get(
        "/localnodes",
        [FakeAlternatorResponse.json(["127.0.0.1"], status=code) for code in statuses],
    )
    fetcher = create_sync_http_fetcher(timeout_seconds=1)
    try:
        for status in statuses:
            assert list(fetcher(server.url("/localnodes"))) == (
                ["127.0.0.1"] if status == 200 else []
            )
        requests = server.captured_requests()
        assert len(requests) == 3
        assert len({request.connection_id for request in requests}) == 1
    finally:
        fetcher.close()
    assert_peer_closed(server)


@pytest.mark.parametrize("status", [302, 500])
def test_unused_response_is_drained_without_retaining_body(
    fake_alternator_server: FakeAlternatorServer,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    server = fake_alternator_server
    server.script_get(
        "/localnodes",
        [
            FakeAlternatorResponse(status, b"x" * (256 * 1024), {"X-Test": "kept"}),
            FakeAlternatorResponse.json(["127.0.0.1"]),
        ],
    )
    original_readinto = http.client.HTTPResponse.readinto
    buffers: list[bytearray] = []

    def readinto(response: http.client.HTTPResponse, buffer: bytearray) -> int:
        if response.status == status:
            buffers.append(buffer)
        return original_readinto(response, buffer)

    monkeypatch.setattr(http.client.HTTPResponse, "readinto", readinto)
    pool = SyncConnectionPool()
    token = pool.begin()
    request = Request(server.url("/localnodes"))
    try:

        def open_response() -> http.client.HTTPResponse:
            return pool.open(
                "http",
                request,
                lambda timeout: http.client.HTTPConnection(
                    request.host, timeout=timeout
                ),
                time.monotonic() + 1,
                token,
            )

        with open_response() as response:
            assert response.status == status
            assert response.headers["X-Test"] == "kept"
            assert response.read() == b""
        assert buffers and len({id(buffer) for buffer in buffers}) == 1
        assert all(len(buffer) == 64 * 1024 for buffer in buffers)
        with open_response() as response:
            assert response.read() == b'["127.0.0.1"]'
        assert len({r.connection_id for r in server.captured_requests()}) == 1
    finally:
        pool.finish(token)
        pool.close()
    assert_peer_closed(server)


@pytest.mark.parametrize("malformed", [False, True])
def test_chunked_error_drain_uses_bounded_decoder_buffers(malformed: bool) -> None:
    buffers: list[tuple[int, int]] = []

    class BoundedStream(io.BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            assert size is not None and size >= 0, "unbounded lower body read"
            return super().read(size)

        def readinto(self, buffer: Any) -> int:  # noqa: ANN401 -- BytesIO accepts arbitrary writable buffers across supported Python versions.
            view = memoryview(buffer)
            buffers.append((id(view.obj), len(view)))
            return super().readinto(buffer)

    chunk = b"-1" if malformed else b"20000"
    stream = BoundedStream(
        b"HTTP/1.1 500 Error\r\nTransfer-Encoding: chunked\r\nX-Test: kept\r\n\r\n"
        + chunk
        + b"\r\n"
        + b"x" * (128 * 1024)
        + b"\r\n0\r\n\r\n"
    )
    transport = Mock(spec=http.client.HTTPConnection)
    transport.sock = Mock(spec=socket.socket)
    response = http.client.HTTPResponse(Mock(makefile=Mock(return_value=stream)))
    response.begin()
    transport.getresponse.return_value = response
    pool = SyncConnectionPool()
    token = pool.begin()
    try:

        def open_response() -> http.client.HTTPResponse:
            return pool.open(
                "http",
                Request("http://seed/localnodes"),
                lambda _timeout: transport,
                time.monotonic() + 1,
                token,
            )

        if malformed:
            with pytest.raises(URLError):
                open_response()
            transport.close.assert_called_once()
            assert not pool._idle
        else:
            with open_response() as drained:
                assert drained.status == 500
                assert drained.headers["X-Test"] == "kept"
                assert drained.read() == b""
            assert len(pool._idle) == 1
        assert buffers and len({owner for owner, size in buffers}) == 1
        assert all(0 < size <= 64 * 1024 for owner, size in buffers)
        assert stream.closed
    finally:
        pool.finish(token)
        pool.close()


@pytest.mark.parametrize("informational", [100, 103])
def test_informational_response_cannot_leave_stale_topology(informational: int) -> None:
    release, final_sent = threading.Event(), threading.Event()
    requests: list[socket.socket] = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            self.connection.settimeout(2)
            with lock:
                requests.append(self.connection)
                first = len(requests) == 1
            try:
                if first:
                    self.send_response_only(informational)
                    self.end_headers()
                    self.wfile.flush()
                    assert release.wait(timeout=2)
                body = b'["127.0.0.2"]' if first else b'["127.0.0.3"]'
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()
            except OSError:
                # Discovery discards the 103 connection before this delayed
                # final write; test cleanup can also close the peer connection.
                pass
            finally:
                if first:
                    final_sent.set()

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    serving = threading.Thread(target=server.serve_forever, daemon=True)
    serving.start()
    fetcher = create_sync_http_fetcher(timeout_seconds=1)
    if informational == 100:
        release.set()
    try:
        url = f"http://127.0.0.1:{server.server_port}/localnodes"
        assert list(fetcher(url)) == (["127.0.0.2"] if informational == 100 else [])
        release.set()
        assert final_sent.wait(timeout=1)
        assert list(fetcher(url)) == ["127.0.0.3"]
        assert len({id(connection) for connection in requests}) == (
            1 if informational == 100 else 2
        )
    finally:
        release.set()
        fetcher.close()
        server.shutdown()
        server.server_close()
        serving.join(timeout=2)


@pytest.mark.parametrize("capacity", [False, True])
def test_idle_pool_bounds_close_discarded_and_retained_sockets(capacity: bool) -> None:
    pool = SyncConnectionPool()
    token = pool.begin()
    connections: list[http.client.HTTPConnection] = []
    sockets: list[Mock] = []

    def create(timeout: float) -> http.client.HTTPConnection:
        connection = http.client.HTTPConnection("seed", timeout=timeout)
        stream = Mock(spec=socket.socket)
        connection.sock = stream
        connections.append(connection)
        sockets.append(stream)
        return connection

    try:
        for index in range(201 if capacity else 2):
            key = ("http", f"seed-{index}" if capacity else "seed", None, None)
            connection, reused = pool._acquire(key, create, time.monotonic() + 1, token)
            assert not reused
            if capacity:
                pool._release(key, connection, token, True)
                assert len(pool._idle) <= 200
                if index == 199:
                    assert len(pool._idle) == 200
                    assert all(not stream.close.called for stream in sockets)
        if capacity:
            assert len(pool._idle) == 200
            assert connections[0].sock is None
            sockets[0].close.assert_called_once()
        else:
            assert len(pool._active) == 2 and not pool._idle
            for connection in connections:
                pool._release(key, connection, token, True)
            assert len(pool._idle) == 1
            assert connections[1].sock is None
            sockets[1].close.assert_called_once()
            retained, reused = pool._acquire(key, create, time.monotonic() + 1, token)
            assert reused and retained is connections[0]
            assert len(connections) == 2
            pool._release(key, retained, token, True)
        pool.close()
        assert not pool._idle and not pool._active
        assert all(connection.sock is None for connection in connections)
        for stream in sockets:
            stream.close.assert_called_once()
    finally:
        pool.finish(token)
        pool.close()


@pytest.mark.parametrize("silent_close", [False, True])
def test_sync_discovery_recovers_closed_connection(
    fake_alternator_server: FakeAlternatorServer, silent_close: bool
) -> None:
    server = fake_alternator_server
    good = FakeAlternatorResponse.json(["127.0.0.1"])
    responses = (
        [good, FakeAlternatorResponse(200, b"", {}, disconnect=True), good]
        if silent_close
        else [
            FakeAlternatorResponse.json(["127.0.0.1"], headers={"Connection": "close"}),
            good,
        ]
    )
    server.script_get("/localnodes", responses)
    fetcher = create_sync_http_fetcher(timeout_seconds=1)
    try:
        for _ in range(2):
            assert list(fetcher(server.url("/localnodes"))) == ["127.0.0.1"]
        requests = server.captured_requests()
        assert len(requests) == (3 if silent_close else 2)
        assert len({request.connection_id for request in requests}) == 2
    finally:
        fetcher.close()
    assert_peer_closed(server)


def test_close_during_dns_does_not_open_a_late_connection(
    fake_alternator_server: FakeAlternatorServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = fake_alternator_server
    entered, release = threading.Event(), threading.Event()
    original_resolve = socket.getaddrinfo
    original_connect = socket.socket.connect
    connects: list[object] = []

    def resolve(
        host: bytes | str | None,
        port: bytes | str | int | None,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[Any]:
        entered.set()
        assert release.wait(timeout=2)
        return original_resolve(host, port, family, type, proto, flags)

    def connect(stream: socket.socket, address: tuple[Any, ...] | str) -> None:
        connects.append(address)
        original_connect(stream, address)

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    monkeypatch.setattr(socket.socket, "connect", connect)
    fetcher = create_sync_http_fetcher(timeout_seconds=1)
    result: list[list[str]] = []
    caller = threading.Thread(
        target=lambda: result.append(list(fetcher(server.url("/localnodes"))))
    )
    try:
        caller.start()
        assert entered.wait(timeout=1)
        fetcher.close()
    finally:
        release.set()
        caller.join(timeout=2)
        fetcher.close()
    assert not caller.is_alive()
    assert result == [[]]
    assert connects == []
    assert server.captured_requests() == []


def test_sync_discovery_keeps_proxy_and_redirect_handling(
    fake_alternator_server: FakeAlternatorServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = fake_alternator_server
    first = "http://entrypoint.test:8000/localnodes"
    redirected = "http://other.test:8000/localnodes"
    server.set_json(first, {}, status=302, headers={"Location": redirected})
    server.set_json(redirected, ["127.0.0.1"])
    monkeypatch.setenv("http_proxy", server.url())
    monkeypatch.setenv("HTTP_PROXY", server.url())
    monkeypatch.setenv("no_proxy", "")
    monkeypatch.setenv("NO_PROXY", "")
    fetcher = create_sync_http_fetcher(timeout_seconds=1)
    try:
        assert list(fetcher(first)) == ["127.0.0.1"]
        assert list(fetcher(first)) == ["127.0.0.1"]
        requests = server.captured_requests()
        assert [request.path for request in requests] == [first, redirected] * 2
        assert len({request.connection_id for request in requests}) == 1
    finally:
        fetcher.close()
    assert_peer_closed(server)


def test_https_proxy_tunnel_keeps_credentials_and_pool_identities_separate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise urllib proxy wiring without replacing its request handlers."""
    connections: list[Mock] = []

    def connection(*args: object, **kwargs: object) -> Mock:
        response = MagicMock(spec=http.client.HTTPResponse)
        response.__enter__.return_value = response
        response.headers = Message()
        response.status, response.reason, response.will_close = 200, "OK", False
        response.read.return_value = b'["127.0.0.1"]'
        transport = Mock(spec=http.client.HTTPSConnection)
        transport.sock = Mock(spec=socket.socket)
        transport.getresponse.return_value = response
        connections.append(transport)
        return transport

    factory = Mock(side_effect=connection)
    monkeypatch.setattr("alternator._http._DeadlineHTTPSConnection", factory)
    monkeypatch.setenv("no_proxy", "")
    monkeypatch.setenv("NO_PROXY", "")
    context = ssl.create_default_context()
    fetcher = create_sync_http_fetcher(context, timeout_seconds=1)
    try:
        for target, user in [
            ("first.test", "user"),
            ("first.test", "user"),
            ("second.test", "user"),
            ("first.test", "other"),
            ("first.test", "user"),
        ]:
            proxy = f"http://{user}:pass@proxy.test:3128"
            monkeypatch.setenv("https_proxy", proxy)
            monkeypatch.setenv("HTTPS_PROXY", proxy)
            assert list(fetcher(f"https://{target}:8443/localnodes")) == ["127.0.0.1"]
        assert factory.call_count == 3
        assert [transport.request.call_count for transport in connections] == [3, 1, 1]
        for call in factory.call_args_list:
            assert call.args == ("proxy.test:3128",)
            assert call.kwargs["context"] is context
        for transport, target, credential in zip(
            connections,
            ["first.test", "second.test", "first.test"],
            ["dXNlcjpwYXNz", "dXNlcjpwYXNz", "b3RoZXI6cGFzcw=="],
            strict=True,
        ):
            transport.set_tunnel.assert_called_once_with(
                f"{target}:8443", headers={"Proxy-Authorization": f"Basic {credential}"}
            )
            for call in transport.request.call_args_list:
                assert call.args[:3] == ("GET", "/localnodes", None)
                assert "Proxy-Authorization" not in call.args[3]
    finally:
        fetcher.close()


def test_sync_discovery_deadline_interrupts_a_dripping_response() -> None:
    """Frequent bytes must not extend the total discovery deadline."""
    finished, disconnected = threading.Event(), threading.Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            body = b'["127.0.0.1"]'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                for byte in body:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(0.02)
            except OSError:
                # The discovery deadline closes the socket while this peer is
                # still writing; that disconnect is the expected test outcome.
                disconnected.set()
            finally:
                finished.set()

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    serving = threading.Thread(target=server.serve_forever, daemon=True)
    serving.start()
    fetcher = create_sync_http_fetcher(timeout_seconds=0.08)
    try:
        started = time.monotonic()
        assert list(fetcher(f"http://127.0.0.1:{server.server_port}/localnodes")) == []
        assert time.monotonic() - started < 0.3
        assert disconnected.wait(timeout=1)
        assert finished.wait(timeout=1)
    finally:
        fetcher.close()
        server.shutdown()
        server.server_close()
        serving.join(timeout=2)


def test_close_interrupts_a_pending_tls_handshake() -> None:
    """Closing discovery cannot wait for the peer's TLS handshake timeout."""
    entered, release = threading.Event(), threading.Event()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(2)
    port = listener.getsockname()[1]

    def peer() -> None:
        with listener.accept()[0] as stream:
            stream.settimeout(2)
            # ClientHello bytes prove do_handshake has started; this peer sends
            # no TLS response, keeping the handshake pending until cancellation.
            assert stream.recv(1) == b"\x16"
            entered.set()
            release.wait(timeout=2)

    server = threading.Thread(target=peer, daemon=True)
    server.start()
    fetcher = create_sync_http_fetcher(ssl.create_default_context(), timeout_seconds=5)
    result: list[list[str]] = []
    caller = threading.Thread(
        target=lambda: result.append(
            list(fetcher(f"https://127.0.0.1:{port}/localnodes"))
        )
    )
    try:
        caller.start()
        assert entered.wait(timeout=1)
        fetcher.close()
        caller.join(timeout=1)
        assert not caller.is_alive()
        assert result == [[]]
        assert not fetcher._pool._active and not fetcher._pool._requests
    finally:
        release.set()
        fetcher.close()
        caller.join(timeout=2)
        server.join(timeout=2)
        listener.close()


@pytest.mark.parametrize("cancellation", ["close", "deadline"])
@pytest.mark.parametrize("scheme", ["http", "https"])
def test_cancellation_interrupts_pending_tcp_connect(
    monkeypatch: pytest.MonkeyPatch, cancellation: str, scheme: str
) -> None:
    """Cancellation must stop the socket operation and worker, not just its caller."""
    entered, interrupted, release = (
        threading.Event(),
        threading.Event(),
        threading.Event(),
    )
    streams: list[socket.socket] = []
    workers: list[threading.Thread] = []

    class PendingSocket(socket.socket):
        def connect(self, address: object) -> None:
            streams.append(self)
            workers.append(threading.current_thread())
            entered.set()
            assert release.wait(timeout=2)
            raise OSError("pending connect interrupted")

        def shutdown(self, how: int) -> None:
            interrupted.set()
            release.set()
            super().shutdown(how)

    monkeypatch.setattr(socket, "socket", PendingSocket)
    fetcher = create_sync_http_fetcher(
        timeout_seconds=0.1 if cancellation == "deadline" else 5
    )
    result: list[list[str]] = []
    caller = threading.Thread(
        target=lambda: result.append(
            list(fetcher(f"{scheme}://127.0.0.1:8000/localnodes"))
        )
    )
    try:
        caller.start()
        assert entered.wait(timeout=1)
        if cancellation == "close":
            fetcher.close()
        assert interrupted.wait(timeout=1)
        workers[0].join(timeout=1)
        caller.join(timeout=1)
        assert not workers[0].is_alive() and not caller.is_alive()
        assert result == [[]]
        assert streams[0].fileno() == -1
        assert not fetcher._pool._active and not fetcher._pool._requests
        assert not fetcher._pool._idle
    finally:
        release.set()
        caller.join(timeout=2)
        for worker in workers:
            worker.join(timeout=2)
        fetcher.close()
