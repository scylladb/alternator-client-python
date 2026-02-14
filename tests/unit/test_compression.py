"""Tests for request body compression."""

import gzip
from unittest.mock import MagicMock

from alternator.core.compression import create_compression_handler


class TestCompressionHandler:
    """Tests for create_compression_handler function."""

    def test_no_compression_below_threshold(self) -> None:
        """Test no compression when body is below threshold."""
        handler = create_compression_handler(1024)
        request = MagicMock()
        request.body = b"small body"
        request.headers = {}

        handler(request)

        assert request.body == b"small body"
        assert "Content-Encoding" not in request.headers

    def test_compression_above_threshold(self) -> None:
        """Test compression applied when body exceeds threshold."""
        handler = create_compression_handler(100)
        request = MagicMock()
        original_body = b"x" * 200
        request.body = original_body
        request.headers = {}

        handler(request)

        # Body should be compressed
        assert request.body != original_body
        assert gzip.decompress(request.body) == original_body
        assert request.headers["Content-Encoding"] == "gzip"
        assert request.headers["Content-Length"] == str(len(request.body))

    def test_no_compression_if_not_beneficial(self) -> None:
        """Test no compression if compressed size >= original."""
        handler = create_compression_handler(10)
        request = MagicMock()
        # Very small data that won't compress well
        original_body = b"abc"
        request.body = original_body
        request.headers = {}

        handler(request)

        # Should not compress if it doesn't reduce size
        # Note: gzip has overhead, so very small data may not benefit
        # The handler checks: len(compressed) >= len(original_body)
        # If compression doesn't help, body stays unchanged
        if request.body != original_body:
            # If it was compressed, verify it actually reduced size
            assert len(request.body) < len(original_body)

    def test_compression_string_body(self) -> None:
        """Test compression of string body."""
        handler = create_compression_handler(100)
        request = MagicMock()
        original_body = "x" * 200
        request.body = original_body
        request.headers = {}

        handler(request)

        # Body should be compressed bytes
        assert isinstance(request.body, bytes)
        assert gzip.decompress(request.body) == original_body.encode("utf-8")
        assert request.headers["Content-Encoding"] == "gzip"

    def test_compression_updates_content_length(self) -> None:
        """Test Content-Length is updated to compressed size."""
        handler = create_compression_handler(100)
        request = MagicMock()
        request.body = b"x" * 1000  # Compressible data
        request.headers = {"Content-Length": "1000"}

        handler(request)

        compressed_len = len(request.body)
        assert request.headers["Content-Length"] == str(compressed_len)
        assert compressed_len < 1000  # Should actually compress

    def test_none_body_no_error(self) -> None:
        """Test handler handles None body gracefully."""
        handler = create_compression_handler(100)
        request = MagicMock()
        request.body = None
        request.headers = {}

        # Should not raise
        handler(request)
        assert request.body is None

    def test_handler_passes_kwargs(self) -> None:
        """Test handler ignores extra kwargs from botocore."""
        handler = create_compression_handler(100)
        request = MagicMock()
        request.body = b"small"
        request.headers = {}

        # Should not raise with extra kwargs
        handler(request, operation_name="PutItem", extra_param="value")
