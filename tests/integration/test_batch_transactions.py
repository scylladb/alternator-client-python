"""Integration tests for batch operations.

These tests require a running Scylla cluster with Alternator enabled.
Start a local cluster with: make scylla-start

"""

import uuid

import pytest

from alternator import AlternatorClient, Config
from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


@pytest.fixture
def config() -> Config:
    """Create test configuration."""
    return Config(
        seed_hosts=[SCYLLA_HOST],
        port=SCYLLA_PORT,
        scheme="http",
    )


@pytest.fixture
def table_name() -> str:
    """Generate unique table name for test isolation."""
    return f"test_batch_{uuid.uuid4().hex[:8]}"


class TestBatchWriteItem:
    """Test BatchWriteItem operations through the load-balanced client."""

    def test_batch_write_multiple_items(self, config: Config, table_name: str) -> None:
        """Test writing multiple items in a single batch."""
        with AlternatorClient(config) as client:
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                # Batch write 10 items
                items = [
                    {
                        "PutRequest": {
                            "Item": {
                                "pk": {"S": f"key_{i}"},
                                "data": {"S": f"value_{i}"},
                            }
                        }
                    }
                    for i in range(10)
                ]
                client.batch_write_item(RequestItems={table_name: items})

                # Verify all items written
                for i in range(10):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"key_{i}"}},
                    )
                    assert response["Item"]["data"]["S"] == f"value_{i}"
            finally:
                client.delete_table(TableName=table_name)

    def test_batch_write_with_deletes(self, config: Config, table_name: str) -> None:
        """Test batch write mixing puts and deletes."""
        with AlternatorClient(config) as client:
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                # Pre-populate items to delete
                for i in range(3):
                    client.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": f"delete_{i}"}, "data": {"S": "old"}},
                    )

                # Batch: put new items + delete old ones
                requests = [
                    {
                        "PutRequest": {
                            "Item": {
                                "pk": {"S": "new_item"},
                                "data": {"S": "fresh"},
                            }
                        }
                    },
                    {"DeleteRequest": {"Key": {"pk": {"S": "delete_0"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "delete_1"}}}},
                ]
                client.batch_write_item(RequestItems={table_name: requests})

                # Verify new item exists
                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "new_item"}},
                )
                assert response["Item"]["data"]["S"] == "fresh"

                # Verify deleted items are gone
                for i in range(2):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"delete_{i}"}},
                    )
                    assert "Item" not in response
            finally:
                client.delete_table(TableName=table_name)


class TestBatchGetItem:
    """Test BatchGetItem operations through the load-balanced client."""

    def test_batch_get_multiple_items(self, config: Config, table_name: str) -> None:
        """Test getting multiple items in a single batch."""
        with AlternatorClient(config) as client:
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                # Write items individually
                for i in range(5):
                    client.put_item(
                        TableName=table_name,
                        Item={
                            "pk": {"S": f"key_{i}"},
                            "data": {"S": f"value_{i}"},
                        },
                    )

                # Batch get all items
                keys = [{"pk": {"S": f"key_{i}"}} for i in range(5)]
                response = client.batch_get_item(
                    RequestItems={table_name: {"Keys": keys}}
                )

                items = response["Responses"][table_name]
                assert len(items) == 5

                # Verify all values present
                returned_keys = {item["pk"]["S"] for item in items}
                expected_keys = {f"key_{i}" for i in range(5)}
                assert returned_keys == expected_keys
            finally:
                client.delete_table(TableName=table_name)

    def test_batch_get_nonexistent_keys(self, config: Config, table_name: str) -> None:
        """Test batch get with keys that don't exist."""
        with AlternatorClient(config) as client:
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                keys = [{"pk": {"S": "nonexistent_key"}}]
                response = client.batch_get_item(
                    RequestItems={table_name: {"Keys": keys}}
                )

                items = response["Responses"].get(table_name, [])
                assert len(items) == 0
            finally:
                client.delete_table(TableName=table_name)
