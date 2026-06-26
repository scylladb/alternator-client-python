"""Shared pytest fixtures and configuration."""

from __future__ import annotations

import contextlib
import json
import threading
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from tests.integration.scylla_version import ScyllaVersion


@dataclass(frozen=True)
class FakeAlternatorResponse:
    """HTTP response configured on the fake Alternator server."""

    status: int
    body: bytes
    headers: Mapping[str, str]


class FakeAlternatorServer:
    """Small deterministic HTTP server for unit tests."""

    def __init__(self) -> None:
        self.requests: list[str] = []
        self._routes: dict[str, FakeAlternatorResponse] = {}
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the server on a local ephemeral port."""
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                owner.requests.append(self.path)
                response = owner._routes.get(
                    self.path,
                    FakeAlternatorResponse(
                        status=404,
                        body=b"not found",
                        headers={"Content-Type": "text/plain"},
                    ),
                )
                self.send_response(response.status)
                for key, value in response.headers.items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(response.body)

            def log_message(self, fmt: str, *args: object) -> None:
                return

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            daemon=True,
            name="fake-alternator-server",
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the server."""
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        self._server = None
        self._thread = None

    def url(self, path: str = "/") -> str:
        """Build an absolute URL for a path on this server."""
        if self._server is None:
            raise RuntimeError("fake Alternator server is not started")
        host, port = self._server.server_address
        return f"http://{host}:{port}{path}"

    def set_json(
        self,
        path: str,
        body: object,
        *,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Configure a JSON response."""
        response_headers = {"Content-Type": "application/json"}
        if headers is not None:
            response_headers.update(headers)
        self._routes[path] = FakeAlternatorResponse(
            status=status,
            body=json.dumps(body).encode("utf-8"),
            headers=response_headers,
        )

    def set_text(
        self,
        path: str,
        body: str,
        *,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Configure a text response."""
        response_headers = {"Content-Type": "text/plain"}
        if headers is not None:
            response_headers.update(headers)
        self._routes[path] = FakeAlternatorResponse(
            status=status,
            body=body.encode("utf-8"),
            headers=response_headers,
        )

    def set_localnodes(
        self,
        nodes: Sequence[str],
        *,
        query: str = "",
        status: int = 200,
    ) -> None:
        """Configure a `/localnodes` JSON response."""
        path = "/localnodes"
        if query:
            path = f"{path}?{query}"
        self.set_json(path, list(nodes), status=status)

    def requested_paths(self) -> list[str]:
        """Return request paths captured by the server."""
        return list(self.requests)

    def requested_queries(self) -> list[str]:
        """Return query strings captured by the server."""
        return [urlsplit(path).query for path in self.requests]


@pytest.fixture
def fake_alternator_server() -> Iterator[FakeAlternatorServer]:
    """Provide a deterministic local HTTP server for Alternator unit tests."""
    server = FakeAlternatorServer()
    server.start()
    try:
        yield server
    finally:
        with contextlib.suppress(Exception):
            server.stop()


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires Scylla)"
    )
    config.addinivalue_line(
        "markers",
        "min_scylla_version(version): skip test if ScyllaDB version is below minimum",
    )


@pytest.fixture(scope="session")
def scylla_version() -> ScyllaVersion | None:
    """Get the running ScyllaDB version.

    This fixture attempts to detect the ScyllaDB version in the following order:
    1. SCYLLA_VERSION environment variable (e.g., "2026.1.0")
    2. Auto-detection from the running cluster

    Returns:
        ScyllaVersion object or None if version cannot be determined

    Usage:
        def test_something(scylla_version):
            if scylla_version and scylla_version >= ScyllaVersion(2026, 1, 0):
                # Test compression feature
                ...
    """
    # Import here to avoid issues when running unit tests only
    try:
        from tests.integration.scylla_version import (
            clear_version_cache,
            get_version_from_env,
        )
    except ImportError:
        return None

    # Clear any cached version from previous runs
    clear_version_cache()

    # First try environment variable
    env_version = get_version_from_env()
    if env_version:
        return env_version

    # Try auto-detection with a temporary client
    from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION

    if SKIP_INTEGRATION:
        return None

    try:
        from alternator import Config, close_client, create_client
        from tests.integration.scylla_version import detect_version_from_cluster

        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            scheme="http",
        )
        client = create_client(config)
        try:
            version = detect_version_from_cluster(client)
            return version
        finally:
            close_client(client)
    except Exception:
        return None


@pytest.fixture
def skip_if_scylla_version_below(
    scylla_version: ScyllaVersion | None,
) -> Callable[..., None]:
    """Factory fixture to skip tests based on ScyllaDB version.

    Usage:
        def test_compression(skip_if_scylla_version_below):
            skip_if_scylla_version_below(ScyllaVersion(2026, 1, 0), "gzip compression")
            # Test code here...
    """
    from tests.integration.scylla_version import (
        ScyllaVersion,  # noqa: F401 - used in type annotation below
        requires_scylla_version,
    )

    def _skip_if_below(min_version: ScyllaVersion, feature_name: str = "") -> None:
        if scylla_version is None:
            pytest.skip(
                f"ScyllaDB version unknown - set SCYLLA_VERSION env var. "
                f"{requires_scylla_version(min_version, feature_name)}"
            )
        if scylla_version < min_version:
            pytest.skip(
                f"ScyllaDB {scylla_version} < {min_version}. "
                f"{requires_scylla_version(min_version, feature_name)}"
            )

    return _skip_if_below
