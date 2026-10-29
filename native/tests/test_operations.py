# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Installed-wheel execution contracts, before the public facade is implemented."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import deepcopy
from typing import Any, Protocol
from urllib.parse import urlsplit

import _alternator_rust as native
import pytest
from botocore.exceptions import ClientError, ParamValidationError

from alternator import Config, RetryConfig, TimeoutConfig
from alternator._experimental.rust._config import PreparedConfig, prepare_config
from alternator._experimental.rust._errors import adapt_error
from tests.contract.cases import (
    ERROR_CASES,
    ITEM,
    KEY,
    TABLE,
    OperationCase,
    operation_cases,
    wire,
)
from tests.fake_server import FakeAlternatorResponse, FakeAlternatorServer

OPERATIONS = {"GetItem", "PutItem", "UpdateItem", "DeleteItem", "Query", "Scan"}
CASES = [case for case in operation_cases() if case.operation in OPERATIONS]


class NativeClient(Protocol):
    def execute(self, operation: str, parameters: dict[str, Any]) -> dict[str, Any]: ...
    def execute_async(
        self, operation: str, parameters: dict[str, Any]
    ) -> asyncio.Future[dict[str, Any]]: ...
    def close(self) -> None: ...
    def close_async(self) -> asyncio.Future[None]: ...
    def _live_drivers(self) -> int: ...


class Entrypoint:
    """Test-only adapter; production facades remain a separate issue."""

    def __init__(self, client: NativeClient, asynchronous: bool) -> None:
        self.client = client
        self.asynchronous = asynchronous

    async def call(self, operation: str, parameters: dict[str, Any]) -> dict[str, Any]:
        if self.asynchronous:
            return await self.client.execute_async(operation, parameters)
        return await asyncio.to_thread(self.client.execute, operation, parameters)


@asynccontextmanager
async def peer(
    asynchronous: bool, *, attempts: int = 1, timeout: float = 3
) -> AsyncIterator[tuple[FakeAlternatorServer, Entrypoint]]:
    server = FakeAlternatorServer()
    server.start()
    server.set_localnodes(["127.0.0.1"])
    config = Config(
        ["127.0.0.1"],
        urlsplit(server.url()).port or 0,
        retries=RetryConfig(max_attempts=attempts),
        timeouts=TimeoutConfig(
            connect_seconds=1, read_seconds=timeout, discovery_seconds=1
        ),
    )
    prepared = prepare_config(config)
    client = None
    try:
        client = (
            await native.create_client_async(prepared.native_config())
            if asynchronous
            else await asyncio.to_thread(native.create_client, prepared.native_config())
        )
        yield server, Entrypoint(client, asynchronous)
    finally:
        try:
            if client is not None:
                if asynchronous:
                    await client.close_async()
                else:
                    await asyncio.to_thread(client.close)
                assert client._live_drivers() == 0
        finally:
            try:
                prepared.close()
            finally:
                await asyncio.to_thread(server.stop)


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("failure", ["close", "drivers", "prepared"])
async def test_peer_cleanup_after_teardown_failure(
    asynchronous: bool, failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    close = native.NativeClient.close
    close_async = native.NativeClient.close_async
    prepared_close = PreparedConfig.close
    server_stop = FakeAlternatorServer.stop
    cleanup_failure = RuntimeError(f"injected {failure} failure")
    body_failure = ValueError("original test failure")
    closed_configs: list[PreparedConfig] = []

    def failing_close(client: NativeClient) -> None:
        close(client)
        raise cleanup_failure

    async def failing_close_async(client: NativeClient) -> None:
        await close_async(client)
        raise cleanup_failure

    def live_drivers(_client: NativeClient) -> int:
        return 1

    def release_config(prepared: PreparedConfig) -> None:
        prepared_close(prepared)
        closed_configs.append(prepared)
        if failure == "prepared":
            raise cleanup_failure

    monkeypatch.setattr(PreparedConfig, "close", release_config)
    if failure == "close":
        monkeypatch.setattr(
            native.NativeClient,
            "close_async" if asynchronous else "close",
            failing_close_async if asynchronous else failing_close,
        )
    elif failure == "drivers":
        monkeypatch.setattr(native.NativeClient, "_live_drivers", live_drivers)
    server = None
    try:
        expected = AssertionError if failure == "drivers" else RuntimeError
        with pytest.raises(expected) as caught:
            async with peer(asynchronous) as (server, _entry):
                raise body_failure
        if failure != "drivers":
            assert caught.value is cleanup_failure
        assert caught.value.__context__ is body_failure
        assert len(closed_configs) == 1
        assert server is not None
        with pytest.raises(RuntimeError, match="not started"):
            server.url()
    finally:
        # Bound the regression probe even if peer cleanup breaks again.
        if server is not None:
            await asyncio.to_thread(server_stop, server)


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.id)
async def test_operation_contract(
    asynchronous: bool, case: OperationCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with peer(asynchronous) as (server, entry):
        # Fail if any Python SDK validation, serialization or transport is used.
        def forbidden(*args: object, **kwargs: object) -> None:
            raise AssertionError("Python SDK executed during native data call")

        for target in (
            "botocore.validate.ParamValidator.validate",
            "botocore.serialize.JSONSerializer.serialize_to_request",
            "botocore.endpoint.Endpoint.make_request",
            "botocore.httpsession.URLLib3Session.send",
            "aiobotocore.endpoint.AioEndpoint.make_request",
            "aiobotocore.httpsession.AIOHTTPSession.send",
        ):
            monkeypatch.setattr(target, forbidden)
        request, response = deepcopy(case.request), deepcopy(case.response)
        server.set_operation(case.operation, wire(response))
        result = await entry.call(case.operation, request)
        metadata = result.pop("ResponseMetadata")
        assert result == response
        assert request == case.request
        assert metadata["HTTPStatusCode"] == 200
        assert metadata["RequestId"] == "contract-request"
        assert metadata["RetryAttempts"] == 0
        captured = server.captured_requests(operation=case.operation)
        assert len(captured) == 1
        assert captured[0].document == wire(request)
        # Every invocation must own its returned Python containers.
        result.clear()
        metadata["HTTPHeaders"].clear()
        again = await entry.call(case.operation, request)
        metadata_again = again.pop("ResponseMetadata")
        assert metadata_again["HTTPHeaders"]
        assert again == response


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("operation", ["Query", "Scan"])
@pytest.mark.parametrize(
    "body",
    [{}, {"Items": []}, {"Count": 0}, {"ScannedCount": 0, "LastEvaluatedKey": {}}],
)
async def test_scalar_presence(
    asynchronous: bool, operation: str, body: dict[str, Any]
) -> None:
    async with peer(asynchronous) as (server, entry):
        server.set_operation(operation, body)
        response = await entry.call(operation, {"TableName": TABLE})
        response.pop("ResponseMetadata")
        assert response == body


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    "code,status,extra",
    [case for case in ERROR_CASES if case[0] != "TransactionCanceledException"],
    ids=lambda value: str(value) if isinstance(value, str) else None,
)
async def test_errors(
    asynchronous: bool, code: str, status: int, extra: dict[str, Any]
) -> None:
    async with peer(asynchronous) as (server, entry):
        server.set_operation("PutItem", {"__type": code, **wire(extra)}, status=status)
        with pytest.raises(native.ServiceError) as caught:
            await entry.call("PutItem", {"TableName": TABLE, "Item": KEY})
        error = caught.value
        assert error.operation_name == "PutItem"
        assert error.response["Error"]["Code"] == code
        assert error.response["ResponseMetadata"]["RetryAttempts"] == 0
        if code == "ConditionalCheckFailedException":
            assert error.response["Item"] == ITEM
        assert isinstance(adapt_error(error, native), ClientError)


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("success", [False, True])
async def test_retries_use_final_attempt(asynchronous: bool, success: bool) -> None:
    async with peer(asynchronous, attempts=2, timeout=10) as (server, entry):
        server.script(
            "GetItem",
            [
                FakeAlternatorResponse.json(
                    {"__type": "InternalServerError", "message": "retry"},
                    status=500,
                    headers={"x-amzn-requestid": "first"},
                ),
                FakeAlternatorResponse.json(
                    {"Item": wire(ITEM)}
                    if success
                    else {"__type": "ResourceNotFoundException", "message": "final"},
                    status=200 if success else 400,
                    headers={"x-amzn-requestid": "last"},
                ),
            ],
        )
        if success:
            response = await entry.call("GetItem", {"TableName": TABLE, "Key": KEY})
            assert response["Item"] == ITEM
        else:
            with pytest.raises(native.ServiceError) as caught:
                await entry.call("GetItem", {"TableName": TABLE, "Key": KEY})
            response = caught.value.response
            assert response["Error"]["Code"] == "ResourceNotFoundException"
        assert response["ResponseMetadata"]["RetryAttempts"] == 1
        assert response["ResponseMetadata"]["RequestId"] == "last"
        assert len(server.captured_requests(operation="GetItem")) == 2


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_validation_unsupported_and_closed(asynchronous: bool) -> None:
    async with peer(asynchronous) as (server, entry):
        with pytest.raises(native.ValidationError) as caught:
            await entry.call("GetItem", {"TableName": TABLE})
        assert isinstance(adapt_error(caught.value, native), ParamValidationError)
        for operation in [
            "BatchGetItem",
            "BatchWriteItem",
            "TransactGetItems",
            "TransactWriteItems",
            "Unknown",
        ]:
            with pytest.raises(NotImplementedError):
                await entry.call(operation, {})
        assert not server.captured_requests(operation="GetItem")
        await asyncio.to_thread(entry.client.close)
        with pytest.raises(RuntimeError, match="closed"):
            await entry.call("GetItem", {"TableName": TABLE, "Key": KEY})


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_close_cancels_held_operation(asynchronous: bool) -> None:
    entered, release = threading.Event(), threading.Event()
    async with peer(asynchronous, timeout=10) as (server, entry):
        server.script(
            "GetItem",
            [FakeAlternatorResponse.json({}, entered=entered, release=release)],
        )
        task = asyncio.create_task(
            entry.call("GetItem", {"TableName": TABLE, "Key": KEY})
        )
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            await asyncio.wait_for(
                asyncio.gather(
                    *(asyncio.to_thread(entry.client.close) for _ in range(3))
                ),
                3,
            )
            with pytest.raises(RuntimeError, match="closed"):
                await asyncio.wait_for(task, 3)
            assert entry.client._live_drivers() == 0
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)


async def test_async_cancellation_and_owned_request() -> None:
    entered, release = threading.Event(), threading.Event()
    async with peer(True, timeout=10) as (server, entry):
        server.script(
            "PutItem",
            [FakeAlternatorResponse.json({}, entered=entered, release=release)],
        )
        value = bytearray(b"original")
        request = {"TableName": TABLE, "Item": {"pk": {"B": value}}}
        future = entry.client.execute_async("PutItem", request)
        value[:] = b"modified"
        request.clear()
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            assert server.captured_requests(operation="PutItem")[0].document == {
                "TableName": TABLE,
                "Item": {"pk": {"B": "b3JpZ2luYWw="}},
            }
            future.cancel()
            with pytest.raises(asyncio.CancelledError):
                await future
            server.set_operation("GetItem", {})
            assert (await entry.call("GetItem", {"TableName": TABLE, "Key": KEY}))[
                "ResponseMetadata"
            ]["RetryAttempts"] == 0
        finally:
            release.set()


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_timeout_endpoint(asynchronous: bool) -> None:
    entered, release = threading.Event(), threading.Event()
    async with peer(asynchronous, timeout=0.2) as (server, entry):
        server.script(
            "GetItem",
            [FakeAlternatorResponse.json({}, entered=entered, release=release)],
        )
        try:
            with pytest.raises(native.TransportError) as caught:
                await entry.call("GetItem", {"TableName": TABLE, "Key": KEY})
            assert caught.value.category == "read_timeout"
            assert caught.value.endpoint_url == server.url()
        finally:
            release.set()


async def test_loop_ownership() -> None:
    async with peer(True) as (_, entry):

        def other_loop() -> None:
            async def run() -> None:
                with pytest.raises(RuntimeError, match="different event loop"):
                    entry.client.execute_async(
                        "GetItem", {"TableName": TABLE, "Key": KEY}
                    )
                with pytest.raises(RuntimeError, match="different event loop"):
                    entry.client.execute("GetItem", {"TableName": TABLE, "Key": KEY})

            asyncio.run(run())

        await asyncio.to_thread(other_loop)


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_final_disconnect_does_not_reuse_previous_response(
    asynchronous: bool,
) -> None:
    async with peer(asynchronous, attempts=2, timeout=10) as (server, entry):
        server.script(
            "GetItem",
            [
                FakeAlternatorResponse.json(
                    {"__type": "InternalServerError"},
                    status=500,
                    headers={"x-amzn-requestid": "stale"},
                ),
                FakeAlternatorResponse(200, b"", {}, disconnect=True),
            ],
        )
        with pytest.raises(native.TransportError) as caught:
            await entry.call("GetItem", {"TableName": TABLE, "Key": KEY})
        assert caught.value.category == "connection_closed"
        assert caught.value.endpoint_url == server.url()
        assert not hasattr(caught.value, "response")
        assert len(server.captured_requests(operation="GetItem")) == 2


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_concurrent_operation_metadata_isolation(asynchronous: bool) -> None:
    async with peer(asynchronous) as (server, entry):
        for index in range(4):
            server.set_operation(
                "GetItem",
                {"Item": {"pk": {"S": str(index)}}},
                table=f"table-{index}",
                headers={"x-amzn-requestid": f"request-{index}"},
            )
        responses = await asyncio.gather(
            *(
                entry.call("GetItem", {"TableName": f"table-{index}", "Key": KEY})
                for index in range(4)
            )
        )
        for index, response in enumerate(responses):
            assert response["Item"] == {"pk": {"S": str(index)}}
            assert response["ResponseMetadata"]["RequestId"] == f"request-{index}"
            assert response["ResponseMetadata"]["RetryAttempts"] == 0
