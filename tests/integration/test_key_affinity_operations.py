"""Integration tests for per-operation-type key affinity routing.

Tests verify that key affinity works correctly for each DynamoDB operation type
with both RMW and ANY_WRITE modes.

These tests require a running Scylla cluster with Alternator enabled.
Start a local cluster with: make scylla-start
"""

import uuid
from typing import Any

import pytest

from alternator import (
    AlternatorConfigBuilder,
    Config,
    KeyRouteAffinityMode,
)
from alternator import (
    client as alternator_client,
)
from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


@pytest.fixture
def table_name() -> str:
    """Generate unique table name for test isolation."""
    return f"test_affinity_{uuid.uuid4().hex[:8]}"


def _create_table(client: Any, table_name: str) -> None:  # noqa: ANN401 -- generated boto3 client type is unavailable in integration tests
    """Helper to create a simple HASH-key table."""
    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = client.get_waiter("table_exists")
    waiter.wait(TableName=table_name)


def _make_rmw_config(table_name: str) -> Config:
    """Create config with RMW affinity mode."""
    return (
        AlternatorConfigBuilder()
        .with_seeds(SCYLLA_HOST)
        .with_port(SCYLLA_PORT)
        .with_key_affinity(
            KeyRouteAffinityMode.RMW,
            table_pk_map={table_name: "pk"},
        )
        .build()
    )


def _make_any_write_config(table_name: str) -> Config:
    """Create config with ANY_WRITE affinity mode."""
    return (
        AlternatorConfigBuilder()
        .with_seeds(SCYLLA_HOST)
        .with_port(SCYLLA_PORT)
        .with_key_affinity(
            KeyRouteAffinityMode.ANY_WRITE,
            table_pk_map={table_name: "pk"},
        )
        .build()
    )


class TestGetOperationAffinity:
    """Test GET operations with affinity (no affinity optimization for reads)."""

    def test_get_item_no_affinity(self, table_name: str) -> None:
        """GET operations should work correctly regardless of affinity mode.

        GetItem is a read operation — in RMW mode it does NOT use affinity,
        in ANY_WRITE mode it also does NOT use affinity. It should still work.
        """
        config = _make_rmw_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "get_test"}, "data": {"S": "value"}},
                )

                # Multiple GETs should all succeed (round-robin, no affinity)
                for _ in range(10):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": "get_test"}},
                    )
                    assert response["Item"]["data"]["S"] == "value"
            finally:
                client.delete_table(TableName=table_name)

    def test_get_item_with_any_write_mode(self, table_name: str) -> None:
        """GET operations with ANY_WRITE mode should use round-robin (no affinity)."""
        config = _make_any_write_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "get_aw"}, "data": {"S": "val"}},
                )

                for _ in range(10):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": "get_aw"}},
                    )
                    assert response["Item"]["data"]["S"] == "val"
            finally:
                client.delete_table(TableName=table_name)


class TestUpdateOperationAffinity:
    """Test UPDATE operations with RMW and ANY_WRITE affinity."""

    def test_update_with_rmw_condition(self, table_name: str) -> None:
        """UpdateItem with ConditionExpression is RMW — should use affinity."""
        config = _make_rmw_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "counter"}, "value": {"N": "0"}},
                )

                for _ in range(5):
                    client.update_item(
                        TableName=table_name,
                        Key={"pk": {"S": "counter"}},
                        UpdateExpression="SET #v = #v + :inc",
                        ConditionExpression="attribute_exists(pk)",
                        ExpressionAttributeNames={"#v": "value"},
                        ExpressionAttributeValues={":inc": {"N": "1"}},
                    )

                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "counter"}},
                )
                assert response["Item"]["value"]["N"] == "5"
            finally:
                client.delete_table(TableName=table_name)

    def test_update_with_any_write(self, table_name: str) -> None:
        """UpdateItem with ANY_WRITE mode routes all updates via affinity."""
        config = _make_any_write_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "aw_counter"}, "value": {"N": "0"}},
                )

                # Plain updates (no condition) — ANY_WRITE uses affinity for these
                for _ in range(5):
                    client.update_item(
                        TableName=table_name,
                        Key={"pk": {"S": "aw_counter"}},
                        UpdateExpression="SET #v = #v + :inc",
                        ExpressionAttributeNames={"#v": "value"},
                        ExpressionAttributeValues={":inc": {"N": "1"}},
                    )

                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "aw_counter"}},
                )
                assert response["Item"]["value"]["N"] == "5"
            finally:
                client.delete_table(TableName=table_name)

    def test_update_with_return_values_rmw(self, table_name: str) -> None:
        """UpdateItem with ReturnValues (non-NONE) is RMW — uses affinity in RMW mode."""
        config = _make_rmw_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "ret_val"}, "data": {"S": "original"}},
                )

                response = client.update_item(
                    TableName=table_name,
                    Key={"pk": {"S": "ret_val"}},
                    UpdateExpression="SET #d = :v",
                    ExpressionAttributeNames={"#d": "data"},
                    ExpressionAttributeValues={":v": {"S": "updated"}},
                    ReturnValues="ALL_OLD",
                )

                assert response["Attributes"]["data"]["S"] == "original"
            finally:
                client.delete_table(TableName=table_name)


class TestDeleteOperationAffinity:
    """Test DELETE operations with RMW and ANY_WRITE affinity."""

    def test_delete_with_rmw_condition(self, table_name: str) -> None:
        """DeleteItem with ConditionExpression is RMW — should use affinity."""
        config = _make_rmw_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "to_delete"}, "version": {"N": "1"}},
                )

                client.delete_item(
                    TableName=table_name,
                    Key={"pk": {"S": "to_delete"}},
                    ConditionExpression="version = :v",
                    ExpressionAttributeValues={":v": {"N": "1"}},
                )

                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "to_delete"}},
                )
                assert "Item" not in response
            finally:
                client.delete_table(TableName=table_name)

    def test_delete_with_any_write(self, table_name: str) -> None:
        """DeleteItem with ANY_WRITE routes all deletes via affinity."""
        config = _make_any_write_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                for i in range(5):
                    client.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": f"del_{i}"}, "data": {"S": f"val_{i}"}},
                    )

                # Delete all items — each delete uses affinity
                for i in range(5):
                    client.delete_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"del_{i}"}},
                    )

                # Verify all deleted
                for i in range(5):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"del_{i}"}},
                    )
                    assert "Item" not in response
            finally:
                client.delete_table(TableName=table_name)


class TestInsertPutItemAffinity:
    """Test INSERT (PutItem) operations with RMW and ANY_WRITE affinity."""

    def test_put_with_rmw_condition(self, table_name: str) -> None:
        """PutItem with ConditionExpression is RMW — should use affinity."""
        config = _make_rmw_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                # Conditional put (insert if not exists) — this is RMW
                client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "new_item"}, "data": {"S": "inserted"}},
                    ConditionExpression="attribute_not_exists(pk)",
                )

                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "new_item"}},
                )
                assert response["Item"]["data"]["S"] == "inserted"

                # Second conditional put should fail (item exists)
                with pytest.raises(client.exceptions.ConditionalCheckFailedException):
                    client.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": "new_item"}, "data": {"S": "duplicate"}},
                        ConditionExpression="attribute_not_exists(pk)",
                    )
            finally:
                client.delete_table(TableName=table_name)

    def test_put_with_any_write(self, table_name: str) -> None:
        """PutItem with ANY_WRITE routes all puts via affinity."""
        config = _make_any_write_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                # Multiple puts with different keys — each routed via affinity
                for i in range(10):
                    client.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": f"put_{i}"}, "data": {"S": f"val_{i}"}},
                    )

                for i in range(10):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"put_{i}"}},
                    )
                    assert response["Item"]["data"]["S"] == f"val_{i}"
            finally:
                client.delete_table(TableName=table_name)


class TestBatchGetAffinity:
    """Test BATCH-GET operations with affinity."""

    def test_batch_get_works_with_affinity(self, table_name: str) -> None:
        """BatchGetItem should work correctly with affinity enabled."""
        config = _make_any_write_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                # Write items
                for i in range(5):
                    client.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": f"batch_{i}"}, "data": {"S": f"val_{i}"}},
                    )

                # Batch get — reads don't use affinity but should still work
                keys = [{"pk": {"S": f"batch_{i}"}} for i in range(5)]
                response = client.batch_get_item(
                    RequestItems={table_name: {"Keys": keys}}
                )

                items = response["Responses"][table_name]
                assert len(items) == 5
                returned_keys = {item["pk"]["S"] for item in items}
                expected_keys = {f"batch_{i}" for i in range(5)}
                assert returned_keys == expected_keys
            finally:
                client.delete_table(TableName=table_name)

    def test_batch_get_with_rmw_mode(self, table_name: str) -> None:
        """BatchGetItem with RMW mode (reads don't trigger affinity)."""
        config = _make_rmw_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                for i in range(3):
                    client.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": f"bg_{i}"}, "data": {"S": f"v_{i}"}},
                    )

                keys = [{"pk": {"S": f"bg_{i}"}} for i in range(3)]
                response = client.batch_get_item(
                    RequestItems={table_name: {"Keys": keys}}
                )

                items = response["Responses"][table_name]
                assert len(items) == 3
            finally:
                client.delete_table(TableName=table_name)


class TestBatchWriteAffinity:
    """Test BATCH-WRITE operations with affinity."""

    def test_batch_write_with_any_write(self, table_name: str) -> None:
        """BatchWriteItem with ANY_WRITE mode routes via affinity."""
        config = _make_any_write_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                items = [
                    {
                        "PutRequest": {
                            "Item": {
                                "pk": {"S": f"bw_{i}"},
                                "data": {"S": f"val_{i}"},
                            }
                        }
                    }
                    for i in range(10)
                ]
                client.batch_write_item(RequestItems={table_name: items})

                # Verify all items
                for i in range(10):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"bw_{i}"}},
                    )
                    assert response["Item"]["data"]["S"] == f"val_{i}"
            finally:
                client.delete_table(TableName=table_name)

    def test_batch_write_with_rmw_mode(self, table_name: str) -> None:
        """BatchWriteItem with RMW mode (batch writes are not RMW)."""
        config = _make_rmw_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                items = [
                    {
                        "PutRequest": {
                            "Item": {
                                "pk": {"S": f"bw_rmw_{i}"},
                                "data": {"S": f"val_{i}"},
                            }
                        }
                    }
                    for i in range(10)
                ]
                client.batch_write_item(RequestItems={table_name: items})

                for i in range(10):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"bw_rmw_{i}"}},
                    )
                    assert response["Item"]["data"]["S"] == f"val_{i}"
            finally:
                client.delete_table(TableName=table_name)

    def test_batch_write_mixed_operations(self, table_name: str) -> None:
        """BatchWriteItem with mixed puts and deletes using affinity."""
        config = _make_any_write_config(table_name)
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_table(client, table_name)
            try:
                # Pre-populate items to delete
                for i in range(3):
                    client.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": f"old_{i}"}, "data": {"S": "old"}},
                    )

                # Batch: put new + delete old
                requests = [
                    {
                        "PutRequest": {
                            "Item": {
                                "pk": {"S": "new_item"},
                                "data": {"S": "fresh"},
                            }
                        }
                    },
                    {"DeleteRequest": {"Key": {"pk": {"S": "old_0"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "old_1"}}}},
                ]
                client.batch_write_item(RequestItems={table_name: requests})

                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "new_item"}},
                )
                assert response["Item"]["data"]["S"] == "fresh"

                for i in range(2):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"old_{i}"}},
                    )
                    assert "Item" not in response
            finally:
                client.delete_table(TableName=table_name)
