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

"""Tests for request body compression."""

import gzip
from unittest.mock import MagicMock, patch

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

    def test_default_gzip_level_is_used(self) -> None:
        """Test default gzip level is passed to gzip.compress."""
        handler = create_compression_handler(10)
        request = MagicMock()
        request.body = b"x" * 200
        request.headers = {}

        with patch("gzip.compress", return_value=b"compressed") as mock_compress:
            handler(request)

        mock_compress.assert_called_once_with(b"x" * 200, compresslevel=9)
        assert request.body == b"compressed"
        assert request.headers["Content-Encoding"] == "gzip"
        assert request.headers["Content-Length"] == str(len(b"compressed"))

    def test_configured_gzip_levels_are_used(self) -> None:
        """Test configured gzip levels are passed to gzip.compress."""
        for level in (0, 1, 6, 9):
            handler = create_compression_handler(10, gzip_level=level)
            request = MagicMock()
            request.body = b"x" * 200
            request.headers = {}

            with patch("gzip.compress", return_value=b"compressed") as mock_compress:
                handler(request)

            mock_compress.assert_called_once_with(b"x" * 200, compresslevel=level)

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


class TestCompressionVariableBodySizes:
    """Test compression behavior with various body sizes."""

    def test_100_bytes_body(self) -> None:
        """Test compression with ~100 byte body."""
        handler = create_compression_handler(50)
        request = MagicMock()
        original_body = b"a" * 100
        request.body = original_body
        request.headers = {}

        handler(request)

        # 100 bytes of repetitive data should compress well
        assert gzip.decompress(request.body) == original_body
        assert request.headers["Content-Encoding"] == "gzip"

    def test_1kb_body(self) -> None:
        """Test compression with 1KB body."""
        handler = create_compression_handler(100)
        request = MagicMock()
        original_body = b"x" * 1024
        request.body = original_body
        request.headers = {}

        handler(request)

        assert gzip.decompress(request.body) == original_body
        assert len(request.body) < 1024
        assert request.headers["Content-Encoding"] == "gzip"
        assert request.headers["Content-Length"] == str(len(request.body))

    def test_10kb_body(self) -> None:
        """Test compression with 10KB body."""
        handler = create_compression_handler(100)
        request = MagicMock()
        original_body = b"y" * (10 * 1024)
        request.body = original_body
        request.headers = {}

        handler(request)

        assert gzip.decompress(request.body) == original_body
        assert len(request.body) < 10 * 1024
        assert request.headers["Content-Encoding"] == "gzip"

    def test_1mb_body(self) -> None:
        """Test compression with 1MB body."""
        handler = create_compression_handler(100)
        request = MagicMock()
        original_body = b"z" * (1024 * 1024)
        request.body = original_body
        request.headers = {}

        handler(request)

        assert gzip.decompress(request.body) == original_body
        assert len(request.body) < 1024 * 1024
        assert request.headers["Content-Encoding"] == "gzip"
        assert request.headers["Content-Length"] == str(len(request.body))


class TestCompressionEmptyBody:
    """Test that empty bodies are not compressed."""

    def test_empty_bytes_body(self) -> None:
        """Test empty bytes body is not compressed."""
        handler = create_compression_handler(0)  # threshold=0 to force attempt
        request = MagicMock()
        request.body = b""
        request.headers = {}

        handler(request)

        # Empty body is below any meaningful threshold
        assert request.body == b""
        assert "Content-Encoding" not in request.headers

    def test_empty_string_body(self) -> None:
        """Test empty string body is not compressed."""
        handler = create_compression_handler(0)
        request = MagicMock()
        request.body = ""
        request.headers = {}

        handler(request)

        # Empty string body: len("".encode()) == 0, which is < min_size=0? No, 0 < 0 is False.
        # With min_size=0, empty body (0 bytes) is NOT < 0, so compression is attempted.
        # But gzip of empty bytes produces overhead, so compressed >= original.
        # Body should remain unchanged (empty string or unchanged).
        assert "Content-Encoding" not in request.headers


class TestCompressionHeaderPreservation:
    """Test that essential headers are preserved during compression."""

    def test_existing_headers_preserved(self) -> None:
        """Test that pre-existing headers are not removed by compression."""
        handler = create_compression_handler(100)
        request = MagicMock()
        request.body = b"x" * 200
        request.headers = {
            "Host": "localhost:9998",
            "X-Amz-Target": "DynamoDB_20120810.PutItem",
            "Content-Type": "application/x-amz-json-1.0",
            "Content-Length": "200",
            "Authorization": "AWS4-HMAC-SHA256 ...",
        }

        handler(request)

        # Compression should only add/update compression-related headers
        assert request.headers["Host"] == "localhost:9998"
        assert request.headers["X-Amz-Target"] == "DynamoDB_20120810.PutItem"
        assert request.headers["Content-Type"] == "application/x-amz-json-1.0"
        assert request.headers["Authorization"] == "AWS4-HMAC-SHA256 ..."
        # Content-Length and Content-Encoding should be updated
        assert request.headers["Content-Encoding"] == "gzip"
        assert request.headers["Content-Length"] == str(len(request.body))

    def test_content_encoding_set_correctly(self) -> None:
        """Test Content-Encoding header is set to 'gzip' after compression."""
        handler = create_compression_handler(100)
        request = MagicMock()
        request.body = b"a" * 500
        request.headers = {}

        handler(request)

        assert request.headers["Content-Encoding"] == "gzip"

    def test_content_length_matches_compressed_body(self) -> None:
        """Test Content-Length matches the actual compressed body size."""
        handler = create_compression_handler(100)
        request = MagicMock()
        request.body = b"b" * 2000
        request.headers = {"Content-Length": "2000"}

        handler(request)

        assert int(request.headers["Content-Length"]) == len(request.body)
        assert int(request.headers["Content-Length"]) < 2000
