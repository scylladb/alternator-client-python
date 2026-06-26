"""Tests for HTTP fetcher and SSL context creation."""

import json
import ssl
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from unittest.mock import patch

import pytest

from alternator._http import create_ssl_context, create_sync_http_fetcher
from alternator.config import TLS, TlsSessionCacheConfig


class MockHTTPHandler(BaseHTTPRequestHandler):
    """Mock HTTP handler for testing."""

    response_data: list[str] = []
    response_code: int = 200

    def do_GET(self) -> None:
        self.send_response(self.response_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(self.response_data).encode())

    def log_message(self, format: str, *args: object) -> None:
        pass  # Suppress logging


@pytest.fixture
def mock_server() -> Generator[HTTPServer, None, None]:
    """Create a mock HTTP server."""
    server = HTTPServer(("127.0.0.1", 0), MockHTTPHandler)
    thread = Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    yield server
    server.shutdown()


class TestCreateSyncHttpFetcher:
    """Tests for create_sync_http_fetcher."""

    def test_fetches_json_node_list(self, mock_server: HTTPServer) -> None:
        """Test fetcher parses JSON node list."""
        MockHTTPHandler.response_data = ["192.168.1.1", "192.168.1.2", "192.168.1.3"]
        MockHTTPHandler.response_code = 200

        fetcher = create_sync_http_fetcher()
        host, port = mock_server.server_address
        url = f"http://{host}:{port}/localnodes"

        nodes = fetcher(url)

        assert list(nodes) == ["192.168.1.1", "192.168.1.2", "192.168.1.3"]

    def test_returns_empty_on_empty_response(self, mock_server: HTTPServer) -> None:
        """Test fetcher returns empty list for empty response."""
        MockHTTPHandler.response_data = []
        MockHTTPHandler.response_code = 200

        fetcher = create_sync_http_fetcher()
        host, port = mock_server.server_address
        url = f"http://{host}:{port}/localnodes"

        nodes = fetcher(url)

        assert list(nodes) == []

    def test_handles_connection_error(self) -> None:
        """Test fetcher handles connection errors gracefully."""
        fetcher = create_sync_http_fetcher()
        # Use a port that's definitely not listening
        url = "http://127.0.0.1:59999/localnodes"

        nodes = fetcher(url)

        # Should return empty list, not raise
        assert list(nodes) == []

    def test_handles_invalid_json(self, mock_server: HTTPServer) -> None:
        """Test fetcher handles invalid JSON gracefully."""

        class BadJSONHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b"not valid json")

            def log_message(self, format: str, *args: object) -> None:
                pass

        server = HTTPServer(("127.0.0.1", 0), BadJSONHandler)
        thread = Thread(target=server.handle_request)
        thread.start()

        fetcher = create_sync_http_fetcher()
        host, port = server.server_address
        url = f"http://{host}:{port}/localnodes"

        nodes = fetcher(url)
        thread.join()

        assert list(nodes) == []

    def test_returns_empty_on_http_404(self, mock_server: HTTPServer) -> None:
        """Test fetcher returns empty list on 404 response."""
        MockHTTPHandler.response_data = []
        MockHTTPHandler.response_code = 404

        fetcher = create_sync_http_fetcher()
        host, port = mock_server.server_address
        url = f"http://{host}:{port}/localnodes"

        nodes = fetcher(url)

        assert list(nodes) == []

    def test_returns_empty_on_http_500(self, mock_server: HTTPServer) -> None:
        """Test fetcher returns empty list on 500 response."""
        MockHTTPHandler.response_data = []
        MockHTTPHandler.response_code = 500

        fetcher = create_sync_http_fetcher()
        host, port = mock_server.server_address
        url = f"http://{host}:{port}/localnodes"

        nodes = fetcher(url)

        assert list(nodes) == []

    def test_returns_empty_on_http_503(self, mock_server: HTTPServer) -> None:
        """Test fetcher returns empty list on 503 response."""
        MockHTTPHandler.response_data = []
        MockHTTPHandler.response_code = 503

        fetcher = create_sync_http_fetcher()
        host, port = mock_server.server_address
        url = f"http://{host}:{port}/localnodes"

        nodes = fetcher(url)

        assert list(nodes) == []

    def test_handles_timeout(self) -> None:
        """Test fetcher handles timeout gracefully."""
        fetcher = create_sync_http_fetcher(timeout_seconds=0.1)
        # Use a non-routable IP to trigger timeout
        url = "http://192.0.2.1:8000/localnodes"

        nodes = fetcher(url)

        assert list(nodes) == []


class TestCreateSslContext:
    """Tests for create_ssl_context."""

    def test_trust_all_disables_verification(self) -> None:
        """Test trust_all mode disables certificate verification."""
        tls_config = TLS.trust_all()
        context = create_ssl_context(tls_config)

        assert context.verify_mode == ssl.CERT_NONE
        assert context.check_hostname is False

    def test_system_default_enables_verification(self) -> None:
        """Test system_default enables certificate verification."""
        tls_config = TLS.system_default()
        context = create_ssl_context(tls_config)

        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is True

    def test_custom_ca_loads_certificates(self, tmp_path: Path) -> None:
        """Test custom CA certificates are loaded."""
        # Create a dummy cert file (not a real cert, just testing the load path)
        # In a real test, we'd use a valid certificate
        cert_file = tmp_path / "ca.pem"

        # Create a minimal self-signed cert for testing
        # This is a valid PEM structure but not a real CA
        # We'll test the path is called correctly using mock
        with patch("ssl.SSLContext.load_verify_locations") as mock_load:
            tls_config = TLS.with_custom_ca(cert_file)
            create_ssl_context(tls_config)
            mock_load.assert_called_once_with(str(cert_file))

    def test_verify_hostname_can_be_disabled(self) -> None:
        """Test hostname verification can be disabled."""
        tls_config = TLS(
            trust_system_ca_certs=True,
            verify_hostname=False,
        )
        context = create_ssl_context(tls_config)

        assert context.check_hostname is False

    def test_session_cache_enabled_by_default(self) -> None:
        """Test session caching is enabled by default."""
        tls_config = TLS.system_default()
        context = create_ssl_context(tls_config)

        # OP_NO_TICKET should NOT be set when session cache is enabled
        assert not (context.options & ssl.OP_NO_TICKET)

    def test_session_cache_custom_config(self) -> None:
        """Test custom session cache configuration."""
        cache_config = TlsSessionCacheConfig(
            enabled=True,
            cache_size=512,
            timeout_seconds=3600,
        )
        tls_config = TLS(session_cache=cache_config)
        context = create_ssl_context(tls_config)

        # Should not raise and should have reasonable settings
        assert context is not None

    def test_session_cache_disabled(self) -> None:
        """Test session caching can be disabled."""
        cache_config = TlsSessionCacheConfig(enabled=False)
        tls_config = TLS(session_cache=cache_config)
        context = create_ssl_context(tls_config)

        # OP_NO_TICKET should be set when session cache is disabled
        assert context.options & ssl.OP_NO_TICKET
