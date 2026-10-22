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

"""Wire contracts for removing redundant SDK header construction."""

from __future__ import annotations

import gzip
import importlib
import io
import json
from collections.abc import Callable, Iterator
from typing import Any, cast
from unittest.mock import AsyncMock
from urllib.parse import urlsplit

import pytest
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSPreparedRequest, AWSRequest, AWSResponse
from botocore.credentials import Credentials

from alternator import (
    Auth,
    CompressionAlgorithm,
    Config,
    HeaderOptimizationConfig,
    RequestCompressionConfig,
    ResponseCompression,
    RetryConfig,
    close_client,
    create_client,
    create_resource,
)
from alternator.async_client import close_async_client, create_async_client
from alternator.config import DEFAULT_USER_AGENT

TABLE_NAME = "table" + "x" * 100


class _Body(io.BytesIO):
    def stream(self, amt: int = 1024, **kwargs: object) -> Iterator[bytes]:
        while data := self.read(amt):
            yield data


class _AsyncResponse(AWSResponse):
    @property
    async def content(self) -> bytes:  # type: ignore[override] # aiobotocore awaits the response body
        return super().content


def _config(
    user_agent: str | Callable[[str], str] | None = "orders-service/1.0",
    *,
    filtering: bool = False,
) -> Config:
    headers = HeaderOptimizationConfig(
        enabled=filtering,
        whitelist=frozenset({"X-Custom"}),
    )
    return Config(
        seed_hosts=["127.0.0.1", "127.0.0.2"],
        port=9999,
        user_agent=user_agent,
        header_optimization=headers,
        request_compression=RequestCompressionConfig(
            algorithm=CompressionAlgorithm.GZIP, min_size_bytes=0
        ),
        response_compression=(ResponseCompression.GZIP,),
        retries=RetryConfig(max_attempts=2),
    )


def _headers(request: AWSPreparedRequest) -> dict[str, str]:
    return {
        k.lower(): v.decode() if isinstance(v, bytes) else v
        for k, v in cast(Any, request.headers).items()
    }


def _check_signature(request: AWSPreparedRequest) -> None:
    headers = _headers(request)
    signed = headers["authorization"].split("SignedHeaders=", 1)[1].split(",", 1)[0]
    reconstructed = AWSRequest(
        method=request.method,
        url=request.url,
        data=request.body,
        headers={k: v for k, v in headers.items() if k in signed.split(";")},
    )
    signer = SigV4Auth(
        Credentials("access", "secret", "token"), "dynamodb", "us-east-1"
    )
    # Re-sign exactly what went over the wire against the routed endpoint.
    reconstructed.context["timestamp"] = headers["x-amz-date"]
    canonical = signer.canonical_request(reconstructed)
    to_sign = signer.string_to_sign(reconstructed, canonical)
    signature = signer.signature(to_sign, reconstructed)
    assert headers["authorization"].endswith(f"Signature={signature}")


def _capture(
    client: object,
    *,
    signed: bool,
    asynchronous: bool = False,
) -> tuple[list[AWSPreparedRequest], list[dict[str, Any]]]:
    """Serve a retry then a compressed success at the real SDK send boundary."""
    sdk_client = cast(Any, client)
    requests: list[AWSPreparedRequest] = []
    contexts: list[dict[str, Any]] = []

    response_cls: Any = _AsyncResponse if asynchronous else AWSResponse

    def context(request: AWSRequest, **kwargs: object) -> None:
        contexts.append(dict(request.context["retries"]))

    def send(request: AWSPreparedRequest, **kwargs: object) -> AWSResponse:
        requests.append(request)
        headers = _headers(request)
        assert urlsplit(str(request.url)).hostname in {"127.0.0.1", "127.0.0.2"}
        assert json.loads(gzip.decompress(cast(bytes, request.body))) == {
            "TableName": TABLE_NAME
        }
        assert headers["content-encoding"] == "gzip"
        assert headers["accept-encoding"] == "gzip"
        assert headers["x-custom"] == "kept"
        assert "amz-sdk-invocation-id" not in headers
        assert "amz-sdk-request" not in headers
        assert "x-amzn-trace-id" not in headers
        if signed:
            assert headers["x-amz-security-token"] == "token"
            _check_signature(request)
        else:
            assert "authorization" not in headers
        if len(requests) == 1:
            return cast(
                AWSResponse,
                response_cls(
                    str(request.url),
                    500,
                    {"content-type": "application/x-amz-json-1.0"},
                    _Body(b'{"__type":"InternalServerError","message":"retry"}'),
                ),
            )
        return cast(
            AWSResponse,
            response_cls(
                str(request.url),
                200,
                {"content-encoding": "gzip", "x-amzn-requestid": "request-id"},
                _Body(
                    gzip.compress(
                        json.dumps({"Table": {"TableName": TABLE_NAME}}).encode()
                    )
                ),
            ),
        )

    def custom(request: AWSRequest, **kwargs: object) -> None:
        request.headers["X-Custom"] = "kept"

    # Keep a custom header even with filtering, so both paths get the same wire contract.
    # It is installed before signing and consequently covered by SigV4.
    sdk_client.meta.events.register("request-created.dynamodb.*", custom)
    sdk_client.meta.events.register("request-created.dynamodb.*", context)
    sdk_client.meta.events.register_last("before-send.dynamodb.*", send)

    def retry(attempts: int, **kwargs: object) -> int | None:
        return 0 if attempts == 1 else None

    sdk_client.meta.events.register_first("needs-retry.dynamodb.DescribeTable", retry)
    return requests, contexts


@pytest.mark.parametrize("signed", [False, True])
@pytest.mark.parametrize("resource", [False, True])
@pytest.mark.parametrize("filtering", [False, True])
def test_sync_factories_preserve_signing_retries_and_compression(
    monkeypatch: pytest.MonkeyPatch,
    signed: bool,
    resource: bool,
    filtering: bool,
) -> None:
    monkeypatch.setattr(
        importlib.import_module("alternator.client"),
        "create_sync_http_fetcher",
        lambda *_args, **_kwargs: lambda _: ["127.0.0.1", "127.0.0.2"],
    )
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "function")
    monkeypatch.setenv("_X_AMZN_TRACE_ID", "Root=test")
    owner: Any = (create_resource if resource else create_client)(
        _config(filtering=filtering),
        auth=Auth.static_credentials("access", "secret", "token") if signed else None,
    )
    client = owner.meta.client if resource else owner
    try:
        requests, contexts = _capture(client, signed=signed)
        result = client.describe_table(TableName=TABLE_NAME)
        assert result["Table"]["TableName"] == TABLE_NAME
        assert result["ResponseMetadata"]["RequestId"] == "request-id"
        assert result["ResponseMetadata"]["RetryAttempts"] == 1
        assert len(requests) == 2
        assert [_headers(r)["user-agent"] for r in requests] == [
            "orders-service/1.0"
        ] * 2
        assert [c["attempt"] for c in contexts] == [1, 2]
        assert contexts[0]["invocation-id"] == contexts[1]["invocation-id"]
    finally:
        close_client(owner)


@pytest.mark.asyncio
@pytest.mark.parametrize("signed", [False, True])
@pytest.mark.parametrize("filtering", [False, True])
async def test_async_factory_preserves_signing_retries_and_compression(
    monkeypatch: pytest.MonkeyPatch, signed: bool, filtering: bool
) -> None:
    monkeypatch.setattr(
        importlib.import_module("alternator.async_client"),
        "create_async_http_fetcher",
        lambda *_args, **_kwargs: AsyncMock(return_value=["127.0.0.1", "127.0.0.2"]),
    )
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "function")
    monkeypatch.setenv("_X_AMZN_TRACE_ID", "Root=test")
    client = await create_async_client(
        _config(filtering=filtering),
        auth=Auth.static_credentials("access", "secret", "token") if signed else None,
    )
    try:
        requests, contexts = _capture(
            client,
            signed=signed,
            asynchronous=True,
        )
        result = await client.describe_table(TableName=TABLE_NAME)
        assert result["Table"]["TableName"] == TABLE_NAME
        assert result["ResponseMetadata"]["RetryAttempts"] == 1
        assert len(requests) == 2
        assert [_headers(r)["user-agent"] for r in requests] == [
            "orders-service/1.0"
        ] * 2
        assert [c["attempt"] for c in contexts] == [1, 2]
        assert contexts[0]["invocation-id"] == contexts[1]["invocation-id"]
    finally:
        await close_async_client(client)


class _Captured(Exception):
    """Stop at the send boundary without making a network request."""


@pytest.mark.parametrize(
    "user_agent", [None, "custom", lambda default: f"prefix {default}"]
)
@pytest.mark.parametrize("resource", [False, True])
def test_sync_skip_avoids_both_ua_builds_and_keeps_configured_value(
    monkeypatch: pytest.MonkeyPatch,
    user_agent: str | Callable[[str], str] | None,
    resource: bool,
) -> None:
    from botocore.useragent import UserAgentString

    monkeypatch.setattr(
        importlib.import_module("alternator.client"),
        "create_sync_http_fetcher",
        lambda *_args, **_kwargs: lambda _: ["127.0.0.1"],
    )
    owner: Any = (create_resource if resource else create_client)(_config(user_agent))
    client = owner.meta.client if resource else owner
    seen = []

    def expensive_ua(self: object) -> str:
        raise AssertionError("SDK User-Agent construction should be skipped")

    def capture(request: AWSPreparedRequest, **kwargs: object) -> None:
        seen.append(_headers(request).get("user-agent"))
        raise _Captured

    # Detect expensive SDK User-Agent construction during serialization and
    # request creation.
    monkeypatch.setattr(UserAgentString, "to_string", expensive_ua)
    client.meta.events.register_last("before-send.dynamodb.*", capture)
    try:
        with pytest.raises(_Captured):
            client.describe_table(TableName=TABLE_NAME)
        expected = (
            user_agent if not callable(user_agent) else user_agent(DEFAULT_USER_AGENT)
        )
        assert seen == [expected]
    finally:
        close_client(owner)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "user_agent", [None, "custom", lambda default: f"prefix {default}"]
)
async def test_async_skip_avoids_both_ua_builds_and_keeps_configured_value(
    monkeypatch: pytest.MonkeyPatch,
    user_agent: str | Callable[[str], str] | None,
) -> None:
    from botocore.useragent import UserAgentString

    monkeypatch.setattr(
        importlib.import_module("alternator.async_client"),
        "create_async_http_fetcher",
        lambda *_args, **_kwargs: AsyncMock(return_value=["127.0.0.1"]),
    )
    client = await create_async_client(_config(user_agent))
    seen = []

    def expensive_ua(self: object) -> str:
        raise AssertionError("SDK User-Agent construction should be skipped")

    def capture(request: AWSPreparedRequest, **kwargs: object) -> None:
        seen.append(_headers(request).get("user-agent"))
        raise _Captured

    monkeypatch.setattr(UserAgentString, "to_string", expensive_ua)
    client.meta.events.register_last("before-send.dynamodb.*", capture)
    try:
        with pytest.raises(_Captured):
            await client.describe_table(TableName=TABLE_NAME)
        expected = (
            user_agent if not callable(user_agent) else user_agent(DEFAULT_USER_AGENT)
        )
        assert seen == [expected]
    finally:
        await close_async_client(client)


def test_skip_is_client_local_and_preserves_custom_trace_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config as BotoConfig
    from botocore.useragent import UserAgentString

    monkeypatch.setattr(
        importlib.import_module("alternator.client"),
        "create_sync_http_fetcher",
        lambda *_args, **_kwargs: lambda _: ["127.0.0.1"],
    )
    optimized = create_client(_config())
    plain = boto3.client(
        "dynamodb",
        endpoint_url="http://localhost:1",
        region_name="us-east-1",
        config=BotoConfig(signature_version=UNSIGNED),
    )
    seen = []

    def trace(params: dict[str, Any], **kwargs: object) -> None:
        params["headers"]["X-Amzn-Trace-Id"] = "custom-trace"

    def capture(request: AWSPreparedRequest, **kwargs: object) -> None:
        seen.append(_headers(request))
        raise _Captured

    optimized.meta.events.register("before-call.dynamodb.*", trace)
    optimized.meta.events.register_last("before-send.dynamodb.*", capture)
    plain.meta.events.register_last("before-send.dynamodb.*", capture)
    try:
        assert (
            cast(Any, plain)._user_agent_creator.to_string.__func__
            is UserAgentString.to_string
        )
        for client in [optimized, plain]:
            with pytest.raises(_Captured):
                client.describe_table(TableName=TABLE_NAME)
        assert seen[0]["x-amzn-trace-id"] == "custom-trace"
        assert "amz-sdk-invocation-id" not in seen[0]
        assert seen[1]["amz-sdk-invocation-id"]
        assert seen[1]["amz-sdk-request"]
    finally:
        close_client(optimized)
        plain.close()
