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
import random
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
        ],
    )
    def test_combined_encodings(self, encoding: str, body: bytes) -> None:
        response: dict[str, Any] = {
            "body": body,
            "headers": {"Content-Encoding": encoding, "Content-Length": "123"},
        }
        create_response_compression_decode_handler()(response)
        assert response == {"body": b"payload", "headers": {}}

    @pytest.mark.parametrize(
        "encoding", ["identity", "identity, identity", " IdEnTiTy , IDENTITY "]
    )
    def test_identity_preserves_content_length(self, encoding: str) -> None:
        body = b"payload"
        response: dict[str, Any] = {
            "body": body,
            "headers": {"Content-Encoding": encoding, "Content-Length": str(len(body))},
        }
        create_response_compression_decode_handler()(response)
        assert response == {"body": body, "headers": {"Content-Length": "7"}}

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
        def custom_decoder(self: gzip.GzipFile, size: int) -> bytes:
            raise RuntimeError("application decoder")

        monkeypatch.setattr(gzip.GzipFile, "read1", custom_decoder)
        with pytest.raises(RuntimeError, match="application decoder"):
            create_response_compression_decode_handler()(
                {
                    "status_code": 503,
                    "body": b"body",
                    "headers": {"Content-Encoding": "gzip"},
                }
            )

    @pytest.mark.parametrize("encoding", ["gzip", "deflate", "identity"])
    @pytest.mark.parametrize("limit", [7, 8])
    def test_accepts_output_at_or_below_limit(self, encoding: str, limit: int) -> None:
        raw = b"payload"
        body = (
            gzip.compress(raw)
            if encoding == "gzip"
            else zlib.compress(raw)
            if encoding == "deflate"
            else raw
        )
        response: dict[str, Any] = {
            "headers": {"Content-Encoding": encoding, "Content-Length": str(len(body))},
            "body": body,
        }

        create_response_compression_decode_handler(limit)(response)

        assert response["body"] == raw
        assert response["headers"] == (
            {"Content-Length": "7"} if encoding == "identity" else {}
        )

    @pytest.mark.parametrize("encoding", ["gzip", "deflate", "identity"])
    @pytest.mark.parametrize("status", [200, 400, 503])
    def test_limit_failure_preserves_original_headers(
        self, encoding: str, status: int
    ) -> None:
        raw = b"payload"
        body = (
            gzip.compress(raw)
            if encoding == "gzip"
            else zlib.compress(raw)
            if encoding == "deflate"
            else raw
        )
        headers = {"Content-Encoding": encoding, "Content-Length": str(len(body))}
        response: dict[str, Any] = {
            "status_code": status,
            "headers": headers.copy(),
            "body": body,
        }
        handler = create_response_compression_decode_handler(6)
        if status >= 400:
            handler(response)
            assert response["body"] == b""
        else:
            with pytest.raises(
                ValueError, match=f"decode {encoding} response: exceeds"
            ):
                handler(response)
            assert response["body"] is body
        assert response["headers"] == headers

    @pytest.mark.parametrize("oversized_layer", ["deflate", "gzip"])
    def test_applies_limit_to_intermediate_and_final_output(
        self, oversized_layer: str
    ) -> None:
        raw = b"{}" if oversized_layer == "deflate" else b"x" * 1000
        inner = gzip.compress(raw)
        limit = len(raw) if oversized_layer == "deflate" else 100
        assert (len(inner) > limit) == (oversized_layer == "deflate")
        body = zlib.compress(inner)
        headers = {
            "Content-Encoding": "gzip, deflate",
            "Content-Length": str(len(body)),
        }
        response: dict[str, Any] = {"body": body, "headers": headers.copy()}

        with pytest.raises(
            ValueError, match=f"decode {oversized_layer} response: exceeds"
        ):
            create_response_compression_decode_handler(limit)(response)

        assert response == {"body": body, "headers": headers}

    def test_gzip_members_share_one_limit(self) -> None:
        body = gzip.compress(b"first") + b"\0" * 5 + gzip.compress(b"second") + b"\0"
        response: dict[str, Any] = {
            "body": body,
            "headers": {"Content-Encoding": "gzip"},
        }
        with pytest.raises(ValueError, match="exceeds"):
            create_response_compression_decode_handler(10)(response)
        assert response["body"] is body

        create_response_compression_decode_handler(11)(response)
        assert response == {"body": b"firstsecond", "headers": {}}

    @pytest.mark.parametrize("body", [b"", gzip.compress(b"")])
    def test_accepts_empty_gzip(self, body: bytes) -> None:
        response: dict[str, Any] = {
            "body": body,
            "headers": {"Content-Encoding": "gzip"},
        }
        create_response_compression_decode_handler(1)(response)
        assert response == {"body": b"", "headers": {}}

    @pytest.mark.parametrize("trailer_offset", [-8, -4, -1])
    def test_gzip_checks_trailer_at_exact_output_limit(
        self, trailer_offset: int
    ) -> None:
        body = bytearray(gzip.compress(b"payload"))
        body[trailer_offset] ^= 1
        response: dict[str, Any] = {
            "body": bytes(body),
            "headers": {"Content-Encoding": "gzip"},
        }
        with pytest.raises(ValueError, match="decode gzip response"):
            create_response_compression_decode_handler(7)(response)

    @pytest.mark.parametrize(
        "encoding,body",
        [
            ("gzip", gzip.compress(b"payload")[:-1]),
            ("gzip", gzip.compress(b"payload") + b"junk"),
            ("deflate", zlib.compress(b"payload")[:-1]),
            ("deflate", zlib.compress(b"payload")[:-1] + b"\0"),
        ],
    )
    def test_rejects_truncated_or_invalid_streams_at_output_limit(
        self, encoding: str, body: bytes
    ) -> None:
        response: dict[str, Any] = {
            "body": body,
            "headers": {"Content-Encoding": encoding},
        }
        with pytest.raises(ValueError, match=f"decode {encoding} response"):
            create_response_compression_decode_handler(7)(response)

    @pytest.mark.parametrize("trailing", [b"junk", zlib.compress(b"ignored")])
    def test_deflate_retains_first_stream_and_trailing_input_behavior(
        self, trailing: bytes
    ) -> None:
        response: dict[str, Any] = {
            "body": zlib.compress(b"payload") + trailing,
            "headers": {"Content-Encoding": "deflate"},
        }
        create_response_compression_decode_handler(7)(response)
        assert response == {"body": b"payload", "headers": {}}

    @pytest.mark.parametrize("level", [0, 6])
    def test_deflate_bounds_input_to_avoid_repeated_large_tail_copies(
        self, level: int, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        raw = random.Random(165).randbytes(512 * 1024)
        body = zlib.compress(raw, level=level)
        decoder = zlib.decompressobj()
        input_sizes: list[int] = []

        class ObservedDeflate:
            def decompress(
                self, data: bytes | memoryview, max_length: int = 0
            ) -> bytes:
                input_sizes.append(len(data))
                return decoder.decompress(data, max_length)

            def __getattr__(self, name: str) -> Any:  # noqa: ANN401 -- decoder proxy
                return getattr(decoder, name)

        monkeypatch.setattr(zlib, "decompressobj", ObservedDeflate)
        response: dict[str, Any] = {
            "body": body,
            "headers": {"Content-Encoding": "deflate"},
        }

        create_response_compression_decode_handler(len(raw))(response)

        assert response == {"body": raw, "headers": {}}
        assert len(input_sizes) > 1
        assert max(input_sizes) <= 64 * 1024
        assert sum(input_sizes) <= 2 * len(body)

    @pytest.mark.parametrize("encoding", ["gzip", "deflate"])
    def test_expansion_uses_bounded_operations_and_stops_at_sentinel(
        self, encoding: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        raw = b"x" * (256 * 1024)
        body = gzip.compress(raw) if encoding == "gzip" else zlib.compress(raw)
        limit = 70_000
        operations: list[tuple[int, int]] = []
        if encoding == "gzip":

            class ObservedGzip(gzip.GzipFile):
                def read1(self, size: int = -1) -> bytes:
                    assert size > 0
                    chunk = super().read1(size)
                    operations.append((size, len(chunk)))
                    return chunk

            monkeypatch.setattr(gzip, "GzipFile", ObservedGzip)
        else:
            decoder = zlib.decompressobj()

            class ObservedDeflate:
                def decompress(self, data: bytes, max_length: int = 0) -> bytes:
                    assert max_length > 0
                    chunk = decoder.decompress(data, max_length)
                    operations.append((max_length, len(chunk)))
                    return chunk

                def flush(self) -> bytes:
                    pytest.fail("flush does not enforce a maximum output length")

                def __getattr__(self, name: str) -> Any:  # noqa: ANN401 -- decoder proxy
                    return getattr(decoder, name)

            monkeypatch.setattr(zlib, "decompressobj", ObservedDeflate)

        response: dict[str, Any] = {
            "body": body,
            "headers": {"Content-Encoding": encoding},
        }
        with pytest.raises(ValueError, match="exceeds"):
            create_response_compression_decode_handler(limit)(response)

        emitted = 0
        assert len(operations) >= 2
        for requested, produced in operations:
            assert 0 < requested <= min(64 * 1024, limit - emitted + 1)
            assert produced <= requested
            emitted += produced
        assert emitted == limit + 1
        assert response["body"] is body
