"""Integration tests for HTTP response compression."""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from alternator import (
    AlternatorConfigBuilder,
    Config,
    ResponseCompression,
)
from alternator import (
    client as alternator_client,
)
from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION
from tests.integration.wire_capture import (
    capture_prepared_requests,
    capture_raw_responses,
    latest_request,
    latest_response,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


def _large_response_payload() -> str:
    return "response-compression-payload-" * 4096


def _response_compression_config(encoding: ResponseCompression) -> Config:
    return (
        AlternatorConfigBuilder()
        .with_seeds(SCYLLA_HOST)
        .with_port(SCYLLA_PORT)
        .with_response_compression(encoding)
        .build()
    )


def _create_table(client: Any, table_name: str) -> None:  # noqa: ANN401 -- SDK clients are dynamically typed
    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    client.get_waiter("table_exists").wait(TableName=table_name)


def _delete_table(client: Any, table_name: str) -> None:  # noqa: ANN401 -- SDK clients are dynamically typed
    client.delete_table(TableName=table_name)
    client.get_waiter("table_not_exists").wait(TableName=table_name)


class TestResponseCompression:
    """Verify response compression behavior on the wire."""

    @pytest.mark.parametrize(
        "encoding",
        [ResponseCompression.GZIP, ResponseCompression.DEFLATE],
    )
    def test_large_get_item_response_is_decoded(
        self,
        encoding: ResponseCompression,
    ) -> None:
        table_name = f"response_compression_{encoding.value}_{uuid.uuid4().hex}"
        with alternator_client(
            "dynamodb",
            cluster_config=_response_compression_config(encoding),
        ) as client:
            captured_requests = capture_prepared_requests(client)
            captured_responses = capture_raw_responses(client, "GetItem")

            _create_table(client, table_name)
            try:
                client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": "response-compression-key"},
                        "data": {"S": _large_response_payload()},
                    },
                )

                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "response-compression-key"}},
                )

                request = latest_request(captured_requests)
                assert request.header("accept-encoding") == encoding.value

                raw_response = latest_response(captured_responses)
                if raw_response.header("content-encoding") is None:
                    pytest.skip(
                        "running Alternator did not return compressed responses; "
                        "requires a build with scylladb/scylladb#27454 enabled"
                    )

                assert raw_response.header("content-encoding") == encoding.value
                assert response["Item"]["data"]["S"] == _large_response_payload()
            finally:
                _delete_table(client, table_name)
