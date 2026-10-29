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

"""Loopback contracts for SDK HTTP error parsing and retry ownership."""

from __future__ import annotations

import gzip
import inspect
import sys
import zlib
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import Any, cast

import pytest
from boto3.dynamodb.types import Binary
from botocore.exceptions import ClientError
from botocore.model import Shape
from botocore.parsers import JSONParser

from alternator import Config, RetryConfig, close_client, close_resource
from alternator.async_client import close_async_client, create_async_client
from alternator.client import create_client, create_resource
from alternator.config import ResponseCompression, ResponseCompressionConfig, RetryMode
from alternator.core.response_parser import normalize_error_response
from tests.fake_server import FakeAlternatorResponse, FakeAlternatorServer


@pytest.fixture(params=["client", "resource", "async"])
async def sdk_client(
    request: pytest.FixtureRequest, contract_config: Config
) -> AsyncIterator[Any]:
    kind, compression, *limits = (
        request.param if isinstance(request.param, tuple) else (request.param, True)
    )
    config = replace(
        contract_config,
        retries=RetryConfig(max_attempts=2),
    )
    if limits:
        config = replace(
            config,
            response_compression=replace(
                config.response_compression, max_decompressed_bytes=limits[0]
            ),
        )
    if compression:
        config = replace(
            config,
            response_compression=replace(
                config.response_compression,
                encodings=(ResponseCompression.GZIP, ResponseCompression.DEFLATE),
            ),
        )
    resource: Any = None
    if kind == "async":
        client: Any = await create_async_client(config)
    elif kind == "resource":
        resource = create_resource(config)
        client = resource.meta.client
    else:
        client = create_client(config)
    try:
        yield client
    finally:
        if kind == "async":
            await close_async_client(client)
        elif resource is not None:
            close_resource(resource)
        else:
            close_client(client)


async def _list_tables(client: Any) -> dict[str, Any]:  # noqa: ANN401 -- SDK client
    result = client.list_tables()
    return cast(dict[str, Any], await result if inspect.isawaitable(result) else result)


_LIMITED_SUCCESS_BODY = b'{"TableNames":["contract"]}'


@pytest.mark.parametrize(
    "sdk_client",
    [
        (kind, True, len(_LIMITED_SUCCESS_BODY))
        for kind in ("client", "resource", "async")
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "outcome",
    ["accepted", "success-overflow", "bad-request", "retry-success", "exhausted"],
)
async def test_configured_response_limit_reaches_sdk_factories(
    sdk_client: Any,  # noqa: ANN401 -- sync/resource/async SDK client
    fake_alternator_server: FakeAlternatorServer,
    outcome: str,
) -> None:
    oversized = gzip.compress(b'{"TableNames":["' + b"x" * 100 + b'"]}')
    accepted = FakeAlternatorResponse(
        200, gzip.compress(_LIMITED_SUCCESS_BODY), {"Content-Encoding": "gzip"}
    )
    status = (
        400
        if outcome == "bad-request"
        else 200
        if outcome == "success-overflow"
        else 503
    )
    responses = [
        FakeAlternatorResponse(
            status,
            oversized,
            {
                "Content-Encoding": "gzip",
                "x-amzn-requestid": f"limit-{attempt}",
                "x-amz-crc32": str(zlib.crc32(oversized)),
            },
        )
        for attempt in (1, 2)
    ]
    if outcome == "accepted":
        responses = [accepted]
    elif outcome == "retry-success":
        responses[1] = accepted
    fake_alternator_server.script("ListTables", responses)

    if outcome in {"accepted", "retry-success"}:
        result = await _list_tables(sdk_client)
        assert result["TableNames"] == ["contract"]
        assert result["ResponseMetadata"]["RetryAttempts"] == (
            outcome == "retry-success"
        )
    elif outcome == "success-overflow":
        with pytest.raises(ValueError, match="decode gzip response: exceeds"):
            await _list_tables(sdk_client)
    else:
        with pytest.raises(ClientError) as caught:
            await _list_tables(sdk_client)
        attempts = 1 if status == 400 else 2
        assert caught.value.response["Error"]["Code"] == str(status)
        metadata = caught.value.response["ResponseMetadata"]
        assert metadata["HTTPStatusCode"] == status
        assert metadata["RetryAttempts"] == attempts - 1
        assert metadata["HTTPHeaders"]["x-amzn-requestid"] == f"limit-{attempts}"
        assert metadata["HTTPHeaders"]["content-encoding"] == "gzip"
    expected_attempts = 2 if outcome in {"retry-success", "exhausted"} else 1
    assert (
        len(fake_alternator_server.captured_requests(operation="ListTables"))
        == expected_attempts
    )


_MALFORMED = [
    (b"[]", {}),
    (b"null", {}),
    (b'"scalar"', {}),
    (b"42", {}),
    (b"true", {}),
    (b"\xff", {}),
    (b"corrupt", {"Content-Encoding": "gzip"}),
    (b"corrupt", {"Content-Encoding": "deflate"}),
    (zlib.compress(b"not-gzip"), {"Content-Encoding": "gzip, deflate"}),
]


@pytest.mark.parametrize("body,headers", _MALFORMED)
@pytest.mark.parametrize("outcome", ["retry-success", "exhausted", "bad-request"])
async def test_malformed_http_errors_use_sdk_retries(
    sdk_client: Any,  # noqa: ANN401 -- sync/resource/async SDK client
    fake_alternator_server: FakeAlternatorServer,
    body: bytes,
    headers: dict[str, str],
    outcome: str,
) -> None:
    status = 400 if outcome == "bad-request" else 503
    responses = [
        FakeAlternatorResponse(
            status, body, headers | {"x-amzn-requestid": f"attempt-{attempt}"}
        )
        for attempt in (1, 2)
    ]
    if outcome == "retry-success":
        responses[1] = FakeAlternatorResponse.json({"TableNames": ["contract"]})
    fake_alternator_server.script("ListTables", responses)
    if outcome == "retry-success":
        result = await _list_tables(sdk_client)
        assert result["TableNames"] == ["contract"]
        assert result["ResponseMetadata"]["RetryAttempts"] == 1
    else:
        with pytest.raises(ClientError) as caught:
            await _list_tables(sdk_client)
        error = caught.value.response
        assert error["Error"]["Code"] == str(status)
        metadata = error["ResponseMetadata"]
        attempts = 1 if status == 400 else 2
        assert metadata["HTTPStatusCode"] == status
        assert metadata["RetryAttempts"] == attempts - 1
        # SDK generic 5xx fallback keeps request ID in HTTPHeaders; its JSON
        # 4xx error path also exposes the convenience RequestId field.
        if status == 400:
            assert metadata["RequestId"] == f"attempt-{attempts}"
        assert metadata["HTTPHeaders"]["x-amzn-requestid"] == f"attempt-{attempts}"
    requests = fake_alternator_server.captured_requests(operation="ListTables")
    assert len(requests) == (1 if status == 400 else 2)


@pytest.mark.parametrize(
    "sdk_client",
    [(kind, False) for kind in ("client", "resource", "async")],
    indirect=True,
)
@pytest.mark.parametrize("body", [b"[]", b"\xff"])
async def test_malformed_http_errors_retry_without_compression(
    sdk_client: Any,  # noqa: ANN401 -- sync/resource/async SDK client
    fake_alternator_server: FakeAlternatorServer,
    body: bytes,
) -> None:
    fake_alternator_server.script(
        "ListTables",
        [
            FakeAlternatorResponse(503, body, {}),
            FakeAlternatorResponse.json({"TableNames": ["contract"]}),
        ],
    )
    result = await _list_tables(sdk_client)
    assert result["TableNames"] == ["contract"]
    assert result["ResponseMetadata"]["RetryAttempts"] == 1
    assert len(fake_alternator_server.captured_requests(operation="ListTables")) == 2


@pytest.mark.parametrize(
    "encoding", ["gzip, deflate", " Deflate , identity , GZIP ", "identity"]
)
@pytest.mark.parametrize("status", [200, 400])
async def test_combined_encoding_success_and_modeled_error(
    sdk_client: Any,  # noqa: ANN401 -- sync/resource/async SDK client
    fake_alternator_server: FakeAlternatorServer,
    encoding: str,
    status: int,
) -> None:
    raw = b'{"Item":{"pk":{"B":"AP8="}},"Attributes":{"pk":{"B":"AP8="}},"__type":"ConditionalCheckFailedException","message":"condition"}'
    if encoding == "identity":
        body = raw
    elif encoding == "gzip, deflate":
        body = zlib.compress(gzip.compress(raw))
    else:
        body = gzip.compress(zlib.compress(raw))
    fake_alternator_server.script(
        "PutItem",
        [FakeAlternatorResponse(status, body, {"Content-Encoding": encoding})],
    )

    async def invoke() -> dict[str, Any]:
        result = sdk_client.put_item(TableName="contract", Item={"pk": {"S": "key"}})
        return cast(
            dict[str, Any], await result if inspect.isawaitable(result) else result
        )

    if status == 400:
        with pytest.raises(
            sdk_client.exceptions.ConditionalCheckFailedException
        ) as caught:
            await invoke()
        result = caught.value.response
        assert result["Error"]["Message"] == "condition"
        assert result["Item"]["pk"]["B"] == b"\x00\xff"
    else:
        result = await invoke()
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
        binary = result["Attributes"]["pk"]
        assert (
            binary == Binary(b"\x00\xff")
            if isinstance(binary, Binary)
            else binary == {"B": b"\x00\xff"}
        )
    headers = result["ResponseMetadata"]["HTTPHeaders"]
    assert "content-encoding" not in headers
    if encoding == "identity":
        assert headers["content-length"] == str(len(body))
    else:
        assert "content-length" not in headers


@pytest.mark.parametrize("mode", list(RetryMode))
@pytest.mark.parametrize("valid_checksum", [False, True])
async def test_checksum_uses_original_compressed_transport_bytes(
    fake_alternator_server: FakeAlternatorServer,
    contract_config: Config,
    mode: RetryMode,
    valid_checksum: bool,
) -> None:
    # SDK retry modes check DynamoDB CRC32 over http_response.content.
    # Normalization must not replace those bytes with the parser's empty body.
    raw = gzip.compress(b"[]")
    checksum = zlib.crc32(raw) if valid_checksum else 0
    fake_alternator_server.script(
        "ListTables",
        [
            FakeAlternatorResponse(
                503, raw, {"Content-Encoding": "gzip", "x-amz-crc32": str(checksum)}
            )
        ],
        fallback=FakeAlternatorResponse.json({"TableNames": []}),
    )
    config = replace(
        contract_config,
        retries=RetryConfig(mode=mode, max_attempts=2),
        response_compression=ResponseCompressionConfig((ResponseCompression.GZIP,)),
    )
    client: Any = create_client(config)
    seen: list[bytes] = []

    def observe(response: Any, **kwargs: Any) -> None:  # noqa: ANN401 -- SDK retry hook
        if response is not None:
            seen.append(response[0].content)

    client.meta.events.register("needs-retry.dynamodb.ListTables", observe)
    try:
        result = client.list_tables()
        assert result["ResponseMetadata"]["RetryAttempts"] == 1
        assert seen[0] == raw
    finally:
        close_client(client)


@pytest.mark.parametrize("body", [b"invalid JSON", b"", b"{}"])
async def test_malformed_success_keeps_sdk_empty_fallback(
    sdk_client: Any,  # noqa: ANN401 -- sync/resource/async SDK client
    fake_alternator_server: FakeAlternatorServer,
    body: bytes,
) -> None:
    fake_alternator_server.script("ListTables", [FakeAlternatorResponse(200, body, {})])
    result = await _list_tables(sdk_client)
    assert set(result) == {"ResponseMetadata"}
    assert result["ResponseMetadata"]["RetryAttempts"] == 0


@pytest.mark.parametrize("body,headers", _MALFORMED)
def test_normalization_does_not_change_malformed_success(
    body: bytes, headers: dict[str, str]
) -> None:
    response = {"status_code": 200, "body": body, "headers": headers.copy()}
    normalize_error_response(response)
    assert response == {"status_code": 200, "body": body, "headers": headers}


@pytest.mark.parametrize("source", ["hook", "converter"])
async def test_application_exceptions_propagate_without_retry(
    sdk_client: Any,  # noqa: ANN401 -- sync/resource/async SDK client
    fake_alternator_server: FakeAlternatorServer,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
) -> None:
    fake_alternator_server.set_operation(
        "PutItem",
        {"__type": "ConditionalCheckFailedException", "Item": {"pk": {"B": "AP8="}}},
        status=400,
    )

    def fail(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401 -- SDK hook/converter
        raise ValueError("application failure")

    if source == "hook":
        sdk_client.meta.events.register("before-parse.dynamodb.PutItem", fail)
    else:
        factory = sdk_client._endpoint._response_parser_factory
        monkeypatch.setitem(factory._original._defaults, "blob_parser", fail)
    with pytest.raises(ValueError, match="application failure"):
        result = sdk_client.put_item(TableName="contract", Item={"pk": {"S": "key"}})
        if inspect.isawaitable(result):
            _ = await result
    assert len(fake_alternator_server.captured_requests(operation="PutItem")) == 1


@pytest.mark.parametrize("status", [200, 400, 503])
@pytest.mark.parametrize(
    "body", [b"plain text", b'{"__type":"Error","message":"failure"}', b" \t\r\n{"]
)
def test_normalization_preserves_sdk_paths(status: int, body: bytes) -> None:
    response = {"status_code": status, "body": body, "headers": {}}
    normalize_error_response(response)
    assert response["body"] is body


def test_invalid_utf8_object_error_uses_sdk_fallback() -> None:
    response = {"status_code": 503, "body": b'{"message":"\xff"}', "headers": {}}
    normalize_error_response(response)
    assert response["body"] == b""
    shape = Shape("Output", {"type": "structure"})
    assert JSONParser().parse(response, shape)["Error"]["Code"] == "503"


def test_normalization_handles_integer_above_conversion_limit() -> None:
    limit = getattr(sys, "get_int_max_str_digits", lambda: 0)()
    if not limit:
        pytest.skip("Interpreter integer conversion limit is unavailable or disabled")
    integer = b"1" * (limit + 1)
    response = {"status_code": 503, "body": integer, "headers": {}}
    parser = JSONParser()
    shape = Shape("Output", {"type": "structure"})
    assert parser.parse(response.copy(), shape)["Error"]["Code"] == "503"

    normalize_error_response(response)

    assert response["body"] == b""
    assert parser.parse(response, shape)["Error"]["Code"] == "503"

    # Inspection must still leave object parsing to the SDK, even when a value
    # exceeds the interpreter's integer conversion limit.
    object_body = b'{"number":' + integer + b"}"
    response["body"] = object_body
    normalize_error_response(response)
    assert response["body"] is object_body


@pytest.mark.parametrize("with_large_integer", [False, True])
def test_normalization_handles_deep_array_roots(with_large_integer: bool) -> None:
    body = b"[" * 10000 + b"]" * 10000
    if with_large_integer:
        limit = getattr(sys, "get_int_max_str_digits", lambda: 0)()
        integer = b"1" * (max(limit, 4300) + 1)
        body = b"[" + integer + b"," + body + b"]"
    response = {"status_code": 503, "body": body, "headers": {}}

    normalize_error_response(response)

    assert response["body"] == b""
    parser = JSONParser()
    shape = Shape("Output", {"type": "structure"})
    assert parser.parse(response, shape)["Error"]["Code"] == "503"

    object_body = b'{"nested":' + body + b"}"
    response["body"] = object_body
    normalize_error_response(response)
    assert response["body"] is object_body


def test_body_decode_application_exception_propagates() -> None:
    class CustomBody(bytes):
        def decode(self, *args: Any, **kwargs: Any) -> str:  # noqa: ANN401 -- bytes override
            raise RuntimeError("application hook")

    with pytest.raises(RuntimeError, match="application hook"):
        normalize_error_response({"status_code": 503, "body": CustomBody(b"body")})
