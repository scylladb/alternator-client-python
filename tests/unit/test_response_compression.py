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

    @pytest.mark.parametrize(
        "encoding,body",
        [
            ("gzip, deflate", zlib.compress(gzip.compress(b"payload"))),
            ("deflate, gzip", gzip.compress(zlib.compress(b"payload"))),
            (" GZip , IDENTITY , DeFLate ", zlib.compress(gzip.compress(b"payload"))),
            ("identity", b"payload"),
        ],
    )
    def test_combined_encodings(self, encoding: str, body: bytes) -> None:
        response: dict[str, Any] = {
            "body": body,
            "headers": {"Content-Encoding": encoding, "Content-Length": "123"},
        }
        create_response_compression_decode_handler()(response)
        assert response == {"body": b"payload", "headers": {}}

    @pytest.mark.parametrize("encoding", ["br, gzip", "gzip, br", "gzip,", ""])
    def test_unknown_chain_is_not_partially_decoded(self, encoding: str) -> None:
        body = gzip.compress(b"payload")
        response: dict[str, Any] = {
            "body": body,
            "headers": {"Content-Encoding": encoding},
        }
        create_response_compression_decode_handler()(response)
        assert response == {"body": body, "headers": {"Content-Encoding": encoding}}

    @pytest.mark.parametrize(
        "encoding,body",
        [
            ("gzip, deflate", zlib.compress(b"not-gzip")),
            ("deflate, gzip", gzip.compress(b"not-deflate")),
        ],
    )
    def test_failed_inner_layer_is_atomic(self, encoding: str, body: bytes) -> None:
        headers = {"Content-Encoding": encoding, "Content-Length": str(len(body))}
        response: dict[str, Any] = {"body": body, "headers": headers.copy()}
        with pytest.raises(ValueError, match="decode .* response"):
            create_response_compression_decode_handler()(response)
        assert response == {"body": body, "headers": headers}

    @pytest.mark.parametrize("encoding", ["gzip", "deflate"])
    def test_invalid_error_body_preserves_headers(self, encoding: str) -> None:
        headers = {"Content-Encoding": encoding, "x-amzn-requestid": "request"}
        response: dict[str, Any] = {
            "status_code": 503,
            "body": b"corrupt",
            "headers": headers.copy(),
        }
        create_response_compression_decode_handler()(response)
        assert response == {"status_code": 503, "body": b"", "headers": headers}

    def test_unexpected_decoder_exception_propagates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def custom_decoder(body: bytes) -> bytes:
            raise RuntimeError("application decoder")

        monkeypatch.setattr(gzip, "decompress", custom_decoder)
        with pytest.raises(RuntimeError, match="application decoder"):
            create_response_compression_decode_handler()(
                {
                    "status_code": 503,
                    "body": b"body",
                    "headers": {"Content-Encoding": "gzip"},
                }
            )
