"""Tests for header filtering functionality."""

from unittest.mock import MagicMock

from alternator.config import AlternatorConfig
from alternator.core.handlers import _register_alternator_handlers
from alternator.core.headers import (
    AUTH_HEADERS,
    BASE_REQUIRED_HEADERS,
    COMPRESSION_HEADERS,
    compute_header_whitelist,
    create_header_filter_handler,
)


class TestComputeHeaderWhitelist:
    """Tests for compute_header_whitelist function."""

    def test_base_headers_always_included(self) -> None:
        """Test base headers are always in whitelist."""
        whitelist = compute_header_whitelist()
        for header in BASE_REQUIRED_HEADERS:
            assert header in whitelist

    def test_auth_headers_when_enabled(self) -> None:
        """Test auth headers included when auth is enabled."""
        whitelist = compute_header_whitelist(auth_enabled=True)
        for header in AUTH_HEADERS:
            assert header in whitelist

    def test_no_auth_headers_when_disabled(self) -> None:
        """Test auth headers excluded when auth is disabled."""
        whitelist = compute_header_whitelist(auth_enabled=False)
        for header in AUTH_HEADERS:
            assert header not in whitelist

    def test_compression_headers_when_enabled(self) -> None:
        """Test compression headers included when compression is enabled."""
        whitelist = compute_header_whitelist(compression_enabled=True)
        for header in COMPRESSION_HEADERS:
            assert header in whitelist

    def test_no_compression_headers_when_disabled(self) -> None:
        """Test compression headers excluded when compression is disabled."""
        whitelist = compute_header_whitelist(compression_enabled=False)
        for header in COMPRESSION_HEADERS:
            assert header not in whitelist

    def test_custom_whitelist_added(self) -> None:
        """Test custom whitelist headers are added."""
        custom = {"X-Custom-Header", "X-Another-Header"}
        whitelist = compute_header_whitelist(custom_whitelist=custom)
        for header in custom:
            assert header in whitelist

    def test_returns_frozenset(self) -> None:
        """Test function returns frozenset (immutable)."""
        whitelist = compute_header_whitelist()
        assert isinstance(whitelist, frozenset)

    def test_all_options_combined(self) -> None:
        """Test all options combined."""
        custom = {"X-Custom"}
        whitelist = compute_header_whitelist(
            auth_enabled=True,
            compression_enabled=True,
            custom_whitelist=custom,
        )

        # All headers should be present
        assert "Host" in whitelist  # base
        assert "Authorization" in whitelist  # auth
        assert "Content-Encoding" in whitelist  # compression
        assert "X-Custom" in whitelist  # custom


class TestHeaderFilterHandler:
    """Tests for create_header_filter_handler function."""

    def test_filters_non_whitelisted_headers(self) -> None:
        """Test non-whitelisted headers are removed."""
        whitelist = frozenset({"Host", "Content-Type"})
        handler = create_header_filter_handler(whitelist)

        request = MagicMock()
        request.headers = {
            "Host": "example.com",
            "Content-Type": "application/json",
            "X-Unwanted": "value",
            "Another-Unwanted": "value",
        }

        handler(request)

        assert "Host" in request.headers
        assert "Content-Type" in request.headers
        assert "X-Unwanted" not in request.headers
        assert "Another-Unwanted" not in request.headers

    def test_case_insensitive_matching(self) -> None:
        """Test header matching is case-insensitive."""
        whitelist = frozenset({"Host", "Content-Type"})
        handler = create_header_filter_handler(whitelist)

        request = MagicMock()
        request.headers = {
            "host": "example.com",  # lowercase
            "CONTENT-TYPE": "application/json",  # uppercase
            "X-Remove": "value",
        }

        handler(request)

        assert "host" in request.headers
        assert "CONTENT-TYPE" in request.headers
        assert "X-Remove" not in request.headers

    def test_empty_whitelist_removes_all(self) -> None:
        """Test empty whitelist removes all headers."""
        whitelist: frozenset[str] = frozenset()
        handler = create_header_filter_handler(whitelist)

        request = MagicMock()
        request.headers = {
            "Host": "example.com",
            "Content-Type": "application/json",
        }

        handler(request)

        assert len(request.headers) == 0

    def test_all_whitelisted_keeps_all(self) -> None:
        """Test all headers kept if all are whitelisted."""
        whitelist = frozenset({"Host", "Content-Type", "Authorization"})
        handler = create_header_filter_handler(whitelist)

        request = MagicMock()
        request.headers = {
            "Host": "example.com",
            "Content-Type": "application/json",
            "Authorization": "Bearer token",
        }

        handler(request)

        assert len(request.headers) == 3

    def test_handles_missing_headers_attribute(self) -> None:
        """Test handler handles request without headers attribute."""
        whitelist = frozenset({"Host"})
        handler = create_header_filter_handler(whitelist)

        request = MagicMock(spec=[])  # No headers attribute

        # Should not raise
        handler(request)

    def test_handler_passes_kwargs(self) -> None:
        """Test handler ignores extra kwargs from botocore."""
        whitelist = frozenset({"Host"})
        handler = create_header_filter_handler(whitelist)

        request = MagicMock()
        request.headers = {"Host": "example.com"}

        # Should not raise with extra kwargs
        handler(request, operation_name="PutItem", extra="value")


class TestBaseRequiredHeaders:
    """Tests for BASE_REQUIRED_HEADERS constant."""

    def test_contains_essential_headers(self) -> None:
        """Test all essential headers are in base set."""
        essential = ["Host", "X-Amz-Target", "Content-Type", "Content-Length"]
        for header in essential:
            assert header in BASE_REQUIRED_HEADERS


class TestAuthHeaders:
    """Tests for AUTH_HEADERS constant."""

    def test_contains_auth_headers(self) -> None:
        """Test auth headers are present."""
        assert "Authorization" in AUTH_HEADERS
        assert "X-Amz-Date" in AUTH_HEADERS
        assert "X-Amz-Security-Token" in AUTH_HEADERS


class TestCompressionHeaders:
    """Tests for COMPRESSION_HEADERS constant."""

    def test_contains_encoding_header(self) -> None:
        """Test Content-Encoding is in compression headers."""
        assert "Content-Encoding" in COMPRESSION_HEADERS


def _make_config(*, optimize_headers: bool = True) -> AlternatorConfig:
    """Create a minimal config for handler registration tests."""
    from alternator.config import HeaderOptimizationConfig

    return AlternatorConfig(
        seed_hosts=("localhost",),
        port=8000,
        header_optimization=HeaderOptimizationConfig(enabled=optimize_headers),
    )


def _make_manager(nodes: tuple[str, ...] = ("127.0.0.1",)) -> MagicMock:
    """Create a mock manager with a nodes property."""
    from alternator.core.live_nodes import NodeList

    manager = MagicMock()
    manager.nodes = NodeList(nodes=nodes, scope_name="cluster")
    return manager


def _make_request(
    url: str = "http://127.0.0.1:8000/",
    *,
    include_auth_headers: bool = True,
) -> MagicMock:
    """Create a mock AWSPreparedRequest with typical headers."""
    request = MagicMock()
    request.url = url
    request.headers = {
        "Host": "127.0.0.1:8000",
        "X-Amz-Target": "DynamoDB_20120810.PutItem",
        "Content-Type": "application/x-amz-json-1.0",
        "Content-Length": "100",
        "User-Agent": "boto3/1.0",
        "Accept-Encoding": "identity",
    }
    if include_auth_headers:
        request.headers.update(
            {
                "Authorization": "AWS4-HMAC-SHA256 Credential=...",
                "X-Amz-Date": "20260214T000000Z",
                "X-Amz-Security-Token": "token123",
            }
        )
    request.body = b'{"TableName": "test", "Item": {"pk": {"S": "val"}}}'
    # No existing query plan
    request._alternator_query_plan = None
    return request


class TestHeaderFilterWithHandlerRegistration:
    """Tests for header filtering through register_alternator_handlers."""

    def test_auth_headers_kept_on_signed_request(self) -> None:
        """Test auth headers are kept when credentials were provided."""
        config = _make_config()
        manager = _make_manager()
        events = MagicMock()

        _register_alternator_handlers(events, manager, config, auth_enabled=True)

        handlers = {
            call[0][1].__name__: call[0][1] for call in events.register.call_args_list
        }

        request = _make_request(include_auth_headers=True)
        handlers["filter_headers"](request)

        assert "Authorization" in request.headers
        assert "X-Amz-Date" in request.headers
        assert "X-Amz-Security-Token" in request.headers

    def test_unsigned_request_strips_auth_headers(self) -> None:
        """Test auth headers are stripped when no credentials were provided."""
        config = _make_config()
        manager = _make_manager()
        events = MagicMock()

        _register_alternator_handlers(events, manager, config, auth_enabled=False)

        handlers = {
            call[0][1].__name__: call[0][1] for call in events.register.call_args_list
        }

        request = _make_request(include_auth_headers=True)
        handlers["filter_headers"](request)

        # Auth headers should be stripped (not whitelisted)
        assert "Authorization" not in request.headers
        assert "X-Amz-Date" not in request.headers
        assert "X-Amz-Security-Token" not in request.headers
        # Base headers should still be present
        assert "Host" in request.headers
        assert "X-Amz-Target" in request.headers
        assert "Content-Type" in request.headers
        # Non-whitelisted headers should also be stripped
        assert "User-Agent" not in request.headers

    def test_no_header_filter_when_optimize_headers_disabled(self) -> None:
        """Test no header filter is registered when optimize_headers=False."""
        config = _make_config(optimize_headers=False)
        manager = _make_manager()
        events = MagicMock()

        _register_alternator_handlers(events, manager, config)

        handler_names = [call[0][1].__name__ for call in events.register.call_args_list]
        assert "filter_headers" not in handler_names
