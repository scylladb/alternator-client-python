"""Tests for HTTP response compression handlers."""

from __future__ import annotations

import gzip
import zlib
from typing import Any
from unittest.mock import MagicMock

import pytest

from alternator.config import ResponseCompression
from alternator.core.compression import (
    create_response_compression_decode_handler,
    create_response_compression_request_handler,
)


class TestResponseCompressionRequestHandler:
    """Tests for adding Accept-Encoding to outgoing requests."""

    def test_sets_accept_encoding_when_missing(self) -> None:
        handler = create_response_compression_request_handler(
            (ResponseCompression.GZIP, ResponseCompression.DEFLATE),
        )
        request = MagicMock()
        request.headers = {}

        handler(request)

        assert request.headers["Accept-Encoding"] == "gzip, deflate"

    def test_replaces_identity_accept_encoding(self) -> None:
        handler = create_response_compression_request_handler(
            (ResponseCompression.DEFLATE,),
        )
        request = MagicMock()
        request.headers = {"Accept-Encoding": "identity"}

        handler(request)

        assert request.headers["Accept-Encoding"] == "deflate"

    def test_preserves_custom_accept_encoding(self) -> None:
        handler = create_response_compression_request_handler(
            (ResponseCompression.GZIP,),
        )
        request = MagicMock()
        request.headers = {"Accept-Encoding": "br"}

        handler(request)

        assert request.headers["Accept-Encoding"] == "br"

    def test_deduplicates_encodings_in_order(self) -> None:
        handler = create_response_compression_request_handler(
            (
                ResponseCompression.GZIP,
                ResponseCompression.DEFLATE,
                ResponseCompression.GZIP,
            ),
        )
        request = MagicMock()
        request.headers = {}

        handler(request)

        assert request.headers["Accept-Encoding"] == "gzip, deflate"


class TestResponseCompressionDecodeHandler:
    """Tests for decoding compressed HTTP responses before SDK parsing."""

    def test_decodes_gzip_response(self) -> None:
        body = b'{"TableNames":["a"]}'
        response_dict = {
            "headers": {
                "Content-Encoding": "gzip",
                "Content-Length": "99",
            },
            "body": gzip.compress(body),
        }
        handler = create_response_compression_decode_handler()

        handler(response_dict=response_dict)

        assert response_dict["body"] == body
        assert "Content-Encoding" not in response_dict["headers"]
        assert "Content-Length" not in response_dict["headers"]

    def test_decodes_deflate_response(self) -> None:
        body = b'{"TableNames":["a"]}'
        response_dict = {
            "headers": {
                "content-encoding": "deflate",
                "content-length": "99",
            },
            "body": zlib.compress(body),
        }
        handler = create_response_compression_decode_handler()

        handler(response_dict=response_dict)

        assert response_dict["body"] == body
        assert "content-encoding" not in response_dict["headers"]
        assert "content-length" not in response_dict["headers"]

    def test_leaves_uncompressed_response_unchanged(self) -> None:
        response_dict = {
            "headers": {"Content-Length": "20"},
            "body": b'{"TableNames":["a"]}',
        }
        handler = create_response_compression_decode_handler()

        handler(response_dict=response_dict)

        assert response_dict == {
            "headers": {"Content-Length": "20"},
            "body": b'{"TableNames":["a"]}',
        }

    def test_ignores_unsupported_content_encoding(self) -> None:
        body = b"compressed"
        response_dict: dict[str, Any] = {
            "headers": {"Content-Encoding": "br"},
            "body": body,
        }
        handler = create_response_compression_decode_handler()

        handler(response_dict=response_dict)

        assert response_dict["body"] == body
        assert response_dict["headers"]["Content-Encoding"] == "br"

    def test_invalid_gzip_response_raises(self) -> None:
        handler = create_response_compression_decode_handler()

        with pytest.raises(ValueError, match="decode gzip response"):
            handler(
                response_dict={
                    "headers": {"Content-Encoding": "gzip"},
                    "body": b"not-gzip",
                }
            )

    def test_invalid_deflate_response_raises(self) -> None:
        handler = create_response_compression_decode_handler()

        with pytest.raises(ValueError, match="decode deflate response"):
            handler(
                response_dict={
                    "headers": {"Content-Encoding": "deflate"},
                    "body": b"not-deflate",
                }
            )
