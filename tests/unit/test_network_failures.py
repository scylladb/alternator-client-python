"""Tests for network failure scenarios."""

from __future__ import annotations

import json
import socket
import time
from collections.abc import Sequence
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import pytest

from alternator._http import create_sync_http_fetcher
from alternator.config import Config
from alternator.core.live_nodes import SyncLiveNodesManager

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class SlowHandler(BaseHTTPRequestHandler):
    """Handler that delays before responding."""

    delay_seconds: float = 5.0

    def do_GET(self) -> None:
        time.sleep(self.delay_seconds)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(["node1"]).encode())

    def log_message(self, format: str, *args: object) -> None:
        pass


class PartialDataHandler(BaseHTTPRequestHandler):
    """Handler that sends partial/truncated JSON."""

    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        # Send truncated JSON — missing closing bracket
        self.wfile.write(b'["node1", "node2"')
        self.wfile.flush()

    def log_message(self, format: str, *args: object) -> None:
        pass


class ConnectionResetHandler(BaseHTTPRequestHandler):
    """Handler that resets the connection after partial headers."""

    def do_GET(self) -> None:
        # Force-close the socket without sending a proper response
        self.connection.close()

    def log_message(self, format: str, *args: object) -> None:
        pass


def _get_url(server: HTTPServer, path: str = "/localnodes") -> str:
    host, port = "127.0.0.1", server.server_port
    return f"http://{host}:{port}{path}"


# ---------------------------------------------------------------------------
# Sync fetcher tests
# ---------------------------------------------------------------------------


class TestSyncFetcherNetworkFailures:
    """Network failure scenarios for the synchronous HTTP fetcher."""

    def test_connection_refused(self) -> None:
        """Fetcher returns empty list when connection is refused."""
        fetcher = create_sync_http_fetcher(timeout_seconds=1.0)
        # Bind a socket, get its port, then close it — guarantees port is unused
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]

        result = fetcher(f"http://127.0.0.1:{port}/localnodes")
        assert list(result) == []

    def test_timeout(self) -> None:
        """Fetcher returns empty list when request times out."""
        SlowHandler.delay_seconds = 5.0
        server = HTTPServer(("127.0.0.1", 0), SlowHandler)
        thread = Thread(target=server.handle_request, daemon=True)
        thread.start()

        fetcher = create_sync_http_fetcher(timeout_seconds=0.3)
        result = fetcher(_get_url(server))

        assert list(result) == []
        # Don't call server.shutdown() — handle_request returns on its own
        # and the daemon thread will be cleaned up at process exit.
        server.server_close()

    def test_partial_data(self) -> None:
        """Fetcher returns empty list when server sends truncated JSON."""
        server = HTTPServer(("127.0.0.1", 0), PartialDataHandler)
        thread = Thread(target=server.handle_request, daemon=True)
        thread.start()

        fetcher = create_sync_http_fetcher(timeout_seconds=2.0)
        result = fetcher(_get_url(server))
        thread.join(timeout=3)

        assert list(result) == []

    def test_connection_reset(self) -> None:
        """Fetcher returns empty list when connection is reset."""
        server = HTTPServer(("127.0.0.1", 0), ConnectionResetHandler)
        thread = Thread(target=server.handle_request, daemon=True)
        thread.start()

        fetcher = create_sync_http_fetcher(timeout_seconds=2.0)
        result = fetcher(_get_url(server))
        thread.join(timeout=3)

        assert list(result) == []

    def test_dns_resolution_failure(self) -> None:
        """Fetcher returns empty list when DNS resolution fails."""
        fetcher = create_sync_http_fetcher(timeout_seconds=1.0)
        result = fetcher("http://this-host-does-not-exist.invalid:8000/localnodes")
        assert list(result) == []


# ---------------------------------------------------------------------------
# Manager-level tests
# ---------------------------------------------------------------------------


class TestSyncManagerNetworkFailures:
    """Network failure scenarios for the SyncLiveNodesManager."""

    @pytest.fixture
    def config(self) -> Config:
        from alternator.config import NodeListPollingConfig

        return Config(
            seed_hosts=["192.168.1.1"],
            port=8000,
            node_list_polling=NodeListPollingConfig(
                active_interval_ms=100, idle_interval_ms=500
            ),
        )

    def test_manager_survives_fetch_exception(self, config: Config) -> None:
        """Manager keeps existing nodes when fetcher raises."""
        call_count = 0

        def flaky_fetch(url: str) -> Sequence[str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return ["node1", "node2"]
            raise ConnectionError("connection reset by peer")

        manager = SyncLiveNodesManager(config, flaky_fetch)
        assert manager.refresh_nodes() is True
        assert len(manager.nodes) == 2

        # Second refresh raises — nodes should remain
        assert manager.refresh_nodes() is False
        assert len(manager.nodes) == 2

    def test_manager_survives_timeout_exception(self, config: Config) -> None:
        """Manager keeps existing nodes when fetcher times out."""
        call_count = 0

        def timeout_fetch(url: str) -> Sequence[str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return ["node1"]
            raise TimeoutError("timed out")

        manager = SyncLiveNodesManager(config, timeout_fetch)
        assert manager.refresh_nodes() is True

        assert manager.refresh_nodes() is False
        assert len(manager.nodes) == 1

    def test_manager_all_seeds_fail(self) -> None:
        """Manager returns False when all seed hosts fail."""
        config = Config(
            seed_hosts=["host1", "host2", "host3"],
            port=8000,
        )

        def always_fail(url: str) -> Sequence[str]:
            raise OSError("network unreachable")

        manager = SyncLiveNodesManager(config, always_fail)
        assert manager.refresh_nodes() is False
        assert len(manager.nodes) == 0

    def test_manager_partial_seed_failure(self) -> None:
        """Manager succeeds if at least one seed host responds."""
        config = Config(
            seed_hosts=["bad-host", "good-host"],
            port=8000,
        )

        def selective_fetch(url: str) -> Sequence[str]:
            if "good-host" in url:
                return ["node1", "node2"]
            raise ConnectionRefusedError("refused")

        manager = SyncLiveNodesManager(config, selective_fetch)
        assert manager.refresh_nodes() is True
        assert len(manager.nodes) == 2


# ---------------------------------------------------------------------------
# Async fetcher tests
# ---------------------------------------------------------------------------


class TestAsyncFetcherNetworkFailures:
    """Network failure scenarios for the async HTTP fetcher."""

    @pytest.fixture
    def unused_port(self) -> int:
        """Get a port that is guaranteed to be unused."""
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
            assert isinstance(port, int)
            return port

    @pytest.mark.asyncio
    async def test_connection_refused(self, unused_port: int) -> None:
        """Async fetcher returns empty list when connection is refused."""
        from alternator._http import create_async_http_fetcher

        fetcher = create_async_http_fetcher(timeout_seconds=1.0)
        try:
            result = await fetcher(f"http://127.0.0.1:{unused_port}/localnodes")
            assert list(result) == []
        finally:
            await fetcher.close()

    @pytest.mark.asyncio
    async def test_timeout(self) -> None:
        """Async fetcher returns empty list when request times out."""
        from alternator._http import create_async_http_fetcher

        SlowHandler.delay_seconds = 5.0
        server = HTTPServer(("127.0.0.1", 0), SlowHandler)
        thread = Thread(target=server.handle_request, daemon=True)
        thread.start()

        fetcher = create_async_http_fetcher(timeout_seconds=0.3)
        try:
            result = await fetcher(_get_url(server))
            assert list(result) == []
        finally:
            await fetcher.close()
            server.server_close()

    @pytest.mark.asyncio
    async def test_dns_resolution_failure(self) -> None:
        """Async fetcher returns empty list when DNS resolution fails."""
        from alternator._http import create_async_http_fetcher

        fetcher = create_async_http_fetcher(timeout_seconds=1.0)
        try:
            result = await fetcher(
                "http://this-host-does-not-exist.invalid:8000/localnodes"
            )
            assert list(result) == []
        finally:
            await fetcher.close()

    @pytest.mark.asyncio
    async def test_partial_data(self) -> None:
        """Async fetcher returns empty list when server sends truncated JSON."""
        from alternator._http import create_async_http_fetcher

        server = HTTPServer(("127.0.0.1", 0), PartialDataHandler)
        thread = Thread(target=server.handle_request, daemon=True)
        thread.start()

        fetcher = create_async_http_fetcher(timeout_seconds=2.0)
        try:
            result = await fetcher(_get_url(server))
            assert list(result) == []
        finally:
            await fetcher.close()
