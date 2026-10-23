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

"""Initial semantic compatibility checks executed by the real Python transports."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError, ParamValidationError

from alternator import Config, RetryConfig
from tests.contract.backends import BackendClient, BackendFactory
from tests.contract.cases import (
    ERROR_CASES,
    KEY,
    TABLE,
    OperationCase,
    operation_cases,
    wire,
)
from tests.fake_server import FakeAlternatorResponse, FakeAlternatorServer


@pytest.mark.parametrize("case", operation_cases(), ids=lambda case: case.id)
async def test_operation_contract(
    backend_factory: BackendFactory,
    contract_config: Config,
    fake_alternator_server: FakeAlternatorServer,
    case: OperationCase,
) -> None:
    expected_request = deepcopy(case.request)
    expected_wire = wire(expected_request)
    expected_response = deepcopy(case.response)
    fake_alternator_server.set_operation(case.operation, wire(expected_response))
    async with backend_factory.client(contract_config) as client:
        response = await client.call(case.method, **case.request)
    assert case.request == expected_request, "backend mutated request"
    metadata = response.pop("ResponseMetadata")
    assert (
        response == expected_response
    )  # preserves absent/empty, set lists and numeric strings
    assert metadata["HTTPStatusCode"] == 200
    assert metadata["RequestId"] == "contract-request"
    assert metadata["RetryAttempts"] == 0
    assert metadata["HTTPHeaders"]["x-amzn-requestid"] == "contract-request"
    requests = fake_alternator_server.captured_requests(operation=case.operation)
    data = [
        r for r in requests if r.table is None or not r.table.startswith(".scylla.")
    ]
    assert len(data) == 1
    assert data[0].document == expected_wire, "backend sent different request"


@pytest.mark.parametrize(
    "code,status,extra", ERROR_CASES, ids=[c[0] for c in ERROR_CASES]
)
async def test_error_contract(
    backend_factory: BackendFactory,
    contract_config: Config,
    fake_alternator_server: FakeAlternatorServer,
    code: str,
    status: int,
    extra: dict[str, Any],
    compatibility_manifest: dict[str, Any],
) -> None:
    operation = (
        "TransactWriteItems" if code == "TransactionCanceledException" else "PutItem"
    )
    logical_body = {"message": "scripted failure", **deepcopy(extra)}
    expected_details = deepcopy(logical_body)
    if not backend_factory.preserves_full_error_payload:
        # Python retains only fields declared by this operation's error shape.
        modeled_members = (
            compatibility_manifest["shapes"][code]["members"]
            if code in compatibility_manifest["operations"][operation]["errors"]
            else {}
        )
        expected_details = {
            key: value
            for key, value in expected_details.items()
            if key in modeled_members
        }
    expected_error = {
        "Error": {"Code": code, "Message": "scripted failure"},
        **expected_details,
    }
    method = "transact_write_items" if operation == "TransactWriteItems" else "put_item"
    kwargs = (
        {"TransactItems": [{"Put": {"TableName": TABLE, "Item": KEY}}]}
        if operation == "TransactWriteItems"
        else {"TableName": TABLE, "Item": KEY}
    )
    fake_alternator_server.set_operation(
        operation,
        {
            "__type": f"com.amazonaws.dynamodb.v20120810#{code}",
            **wire(logical_body),
        },
        status=status,
        headers={"X-Contract-Original": "retained"},
    )
    async with backend_factory.client(contract_config) as client:
        with pytest.raises(ClientError) as caught:
            await client.call(method, **kwargs)
        error = caught.value
        assert error.operation_name == operation
        payload = {
            key: value
            for key, value in error.response.items()
            if key != "ResponseMetadata"
        }
        assert payload == expected_error, "backend changed modeled error payload"
        metadata = error.response["ResponseMetadata"]
        assert metadata["HTTPStatusCode"] == status
        assert metadata["RequestId"] == "contract-request"
        assert metadata["RetryAttempts"] == 0
        assert metadata["HTTPHeaders"]["x-contract-original"] == "retained"
        if code in compatibility_manifest["operations"][operation]["errors"]:
            modeled_class = getattr(client.raw.exceptions, code, None)
            assert modeled_class is not None, "backend missing modeled exception class"
            assert (
                isinstance(modeled_class, type)
                and issubclass(modeled_class, ClientError)
                and modeled_class is not ClientError
            ), "backend exposed invalid modeled exception class"
            assert isinstance(error, modeled_class), (
                "backend raised wrong modeled exception type"
            )


class _FullErrorPayloadFactory(BackendFactory):
    """Exercise native error semantics through a real Python SDK transport."""

    @property
    def preserves_full_error_payload(self) -> bool:
        return True


@pytest.mark.parametrize(
    "code,dropped_member",
    [
        ("UnknownFutureException", "future"),
        ("ValidationException", "message"),
        ("TransactionCanceledException", "message"),
    ],
)
@pytest.mark.parametrize("drop_member", [False, True], ids=["complete", "dropped"])
async def test_full_error_payload_contract_preserves_unmodeled_members(
    backend_factory: BackendFactory,
    contract_config: Config,
    fake_alternator_server: FakeAlternatorServer,
    monkeypatch: pytest.MonkeyPatch,
    compatibility_manifest: dict[str, Any],
    code: str,
    dropped_member: str,
    drop_member: bool,
) -> None:
    code, status, extra = next(case for case in ERROR_CASES if case[0] == code)
    logical_body = {"message": "scripted failure", **deepcopy(extra)}
    real_call = BackendClient.call

    async def retain_body(
        self: BackendClient,
        method: str,
        **kwargs: Any,  # noqa: ANN401 -- SDK kwargs
    ) -> dict[str, Any]:
        try:
            return await real_call(self, method, **kwargs)
        except ClientError as error:
            payload = cast(dict[str, Any], error.response)
            payload.update(deepcopy(logical_body))
            if drop_member:
                del payload[dropped_member]
            raise

    monkeypatch.setattr(BackendClient, "call", retain_body)
    full_payload_factory = _FullErrorPayloadFactory(backend_factory.name)
    if drop_member:
        with pytest.raises(
            AssertionError, match="backend changed modeled error payload"
        ):
            await test_error_contract(
                full_payload_factory,
                contract_config,
                fake_alternator_server,
                code,
                status,
                extra,
                compatibility_manifest,
            )
    else:
        await test_error_contract(
            full_payload_factory,
            contract_config,
            fake_alternator_server,
            code,
            status,
            extra,
            compatibility_manifest,
        )


@pytest.mark.parametrize("mutate_original", [True, False], ids=["mutated", "copied"])
async def test_operation_contract_detects_dropped_request_attribute(
    backend_factory: BackendFactory,
    contract_config: Config,
    fake_alternator_server: FakeAlternatorServer,
    monkeypatch: pytest.MonkeyPatch,
    mutate_original: bool,
) -> None:
    real_call = BackendClient.call

    async def drop_number(
        self: BackendClient,
        method: str,
        **kwargs: Any,  # noqa: ANN401 -- SDK kwargs
    ) -> dict[str, Any]:
        request = kwargs if mutate_original else deepcopy(kwargs)
        request["Item"].pop("number")
        return await real_call(self, method, **request)

    monkeypatch.setattr(BackendClient, "call", drop_number)
    case = next(
        case
        for case in operation_cases()
        if case.method == "put_item" and case.name == "modern"
    )
    failure = (
        "backend mutated request"
        if mutate_original
        else "backend sent different request"
    )
    with pytest.raises(AssertionError, match=failure):
        await test_operation_contract(
            backend_factory, contract_config, fake_alternator_server, case
        )


async def test_error_contract_detects_dropped_modeled_details(
    backend_factory: BackendFactory,
    contract_config: Config,
    fake_alternator_server: FakeAlternatorServer,
    monkeypatch: pytest.MonkeyPatch,
    compatibility_manifest: dict[str, Any],
) -> None:
    real_call = BackendClient.call

    async def drop_reasons(
        self: BackendClient,
        method: str,
        **kwargs: Any,  # noqa: ANN401 -- SDK kwargs
    ) -> dict[str, Any]:
        try:
            return await real_call(self, method, **kwargs)
        except ClientError as error:
            assert cast(dict[str, Any], error.response).pop("ThrottlingReasons")
            raise

    monkeypatch.setattr(BackendClient, "call", drop_reasons)
    code, status, extra = next(
        case
        for case in ERROR_CASES
        if case[0] == "ProvisionedThroughputExceededException"
    )
    with pytest.raises(AssertionError, match="backend changed modeled error payload"):
        await test_error_contract(
            backend_factory,
            contract_config,
            fake_alternator_server,
            code,
            status,
            extra,
            compatibility_manifest,
        )


@pytest.mark.parametrize("broken_type", ["missing", "generic", "generic-class"])
async def test_error_contract_requires_modeled_exception_types(
    backend_factory: BackendFactory,
    contract_config: Config,
    fake_alternator_server: FakeAlternatorServer,
    monkeypatch: pytest.MonkeyPatch,
    compatibility_manifest: dict[str, Any],
    broken_type: str,
) -> None:
    real_call = BackendClient.call

    async def generic_error(
        self: BackendClient,
        method: str,
        **kwargs: Any,  # noqa: ANN401 -- SDK kwargs
    ) -> dict[str, Any]:
        try:
            return await real_call(self, method, **kwargs)
        except ClientError as error:
            if broken_type == "missing":
                self.raw = SimpleNamespace(exceptions=SimpleNamespace())
            elif broken_type == "generic-class":
                self.raw = SimpleNamespace(
                    exceptions=SimpleNamespace(
                        ConditionalCheckFailedException=ClientError
                    )
                )
            raise ClientError(error.response, error.operation_name) from error

    monkeypatch.setattr(BackendClient, "call", generic_error)
    code, status, extra = next(
        case for case in ERROR_CASES if case[0] == "ConditionalCheckFailedException"
    )
    failure = {
        "missing": "backend missing modeled exception class",
        "generic": "backend raised wrong modeled exception type",
        "generic-class": "backend exposed invalid modeled exception class",
    }[broken_type]
    with pytest.raises(AssertionError, match=failure):
        await test_error_contract(
            backend_factory,
            contract_config,
            fake_alternator_server,
            code,
            status,
            extra,
            compatibility_manifest,
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"TableName": TABLE, "Key": KEY, "UnknownParameter": True},
        {"TableName": TABLE},
        {"TableName": TABLE, "Key": "wrong-type"},
    ],
)
async def test_invalid_parameters_fail_before_data_network(
    backend_factory: BackendFactory,
    contract_config: Config,
    fake_alternator_server: FakeAlternatorServer,
    kwargs: dict[str, Any],
) -> None:
    async with backend_factory.client(contract_config) as client:
        with pytest.raises(ParamValidationError):
            await client.call("get_item", **kwargs)
    assert fake_alternator_server.captured_requests(operation="GetItem") == []


async def test_metadata_isolation(
    backend_factory: BackendFactory,
    contract_config: Config,
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    fake_alternator_server.set_operation(
        "GetItem", {}, headers={"x-amzn-requestid": "first"}
    )
    fake_alternator_server.set_operation(
        "PutItem", {}, headers={"X-AMZN-REQUESTID": "second"}
    )
    async with backend_factory.client(contract_config) as client:
        first = await client.call("get_item", TableName=TABLE, Key=KEY)
        second = await client.call("put_item", TableName=TABLE, Item=KEY)
    assert first["ResponseMetadata"]["RequestId"] == "first"
    assert second["ResponseMetadata"]["RequestId"] == "second"
    assert first["ResponseMetadata"]["HTTPHeaders"]["x-amzn-requestid"] == "first"
    assert second["ResponseMetadata"]["HTTPHeaders"]["x-amzn-requestid"] == "second"
    assert first["ResponseMetadata"] is not second["ResponseMetadata"]


async def test_retry_metadata(
    backend_factory: BackendFactory,
    contract_config: Config,
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    fake_alternator_server.script(
        "GetItem",
        [
            FakeAlternatorResponse.json(
                {"__type": "InternalServerError", "message": "retry"}, status=500
            ),
            FakeAlternatorResponse.json(
                {"Item": KEY}, headers={"x-amzn-RequestId": "retried"}
            ),
        ],
    )
    async with backend_factory.client(
        replace(contract_config, retries=RetryConfig(max_attempts=2))
    ) as client:
        response = await client.call("get_item", TableName=TABLE, Key=KEY)
    assert response["Item"] == KEY
    assert response["ResponseMetadata"]["RetryAttempts"] == 1
    assert response["ResponseMetadata"]["RequestId"] == "retried"
    assert len(fake_alternator_server.captured_requests(operation="GetItem")) == 2
