"""Integration tests for batch and transaction operations.

These tests require a running Scylla cluster with Alternator enabled.
Start a local cluster with: make scylla-start

The tests expect Scylla to be available at localhost:8000.
"""

import os
import uuid

import pytest

from alternator import AlternatorClient, AlternatorConfig

SCYLLA_HOST = os.environ.get("SCYLLA_HOST", "localhost")
SCYLLA_PORT = int(os.environ.get("SCYLLA_PORT", "8000"))
SKIP_INTEGRATION = os.environ.get("SKIP_INTEGRATION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


@pytest.fixture
def config() -> AlternatorConfig:
    """Create test configuration."""
    return AlternatorConfig(
        seed_hosts=[SCYLLA_HOST],
        port=SCYLLA_PORT,
        scheme="http",
    )


@pytest.fixture
def table_name() -> str:
    """Generate unique table name for test isolation."""
    return f"test_batch_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def second_table_name() -> str:
    """Generate a second unique table name for multi-table tests."""
    return f"test_batch2_{uuid.uuid4().hex[:8]}"


class TestBatchWriteItem:
    """Test BatchWriteItem operations through the load-balanced client."""

    def test_batch_write_multiple_items(
        self, config: AlternatorConfig, table_name: str
    ) -> None:
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

    def test_batch_write_with_deletes(
        self, config: AlternatorConfig, table_name: str
    ) -> None:
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

    def test_batch_get_multiple_items(
        self, config: AlternatorConfig, table_name: str
    ) -> None:
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

    def test_batch_get_nonexistent_keys(
        self, config: AlternatorConfig, table_name: str
    ) -> None:
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


class TestTransactWriteItems:
    """Test TransactWriteItems operations through the load-balanced client."""

    def test_transact_write_put_items(
        self, config: AlternatorConfig, table_name: str
    ) -> None:
        """Test transactional write of multiple items."""
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
                client.transact_write_items(
                    TransactItems=[
                        {
                            "Put": {
                                "TableName": table_name,
                                "Item": {
                                    "pk": {"S": "txn_1"},
                                    "data": {"S": "value_1"},
                                },
                            }
                        },
                        {
                            "Put": {
                                "TableName": table_name,
                                "Item": {
                                    "pk": {"S": "txn_2"},
                                    "data": {"S": "value_2"},
                                },
                            }
                        },
                    ]
                )

                # Verify both items written
                for i in (1, 2):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"txn_{i}"}},
                    )
                    assert response["Item"]["data"]["S"] == f"value_{i}"
            finally:
                client.delete_table(TableName=table_name)

    def test_transact_write_condition_check_failure(
        self, config: AlternatorConfig, table_name: str
    ) -> None:
        """Test that transactional write with failed condition rolls back."""
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
                # Transaction should fail: ConditionCheck requires item to exist
                with pytest.raises(client.exceptions.TransactionCanceledException):
                    client.transact_write_items(
                        TransactItems=[
                            {
                                "Put": {
                                    "TableName": table_name,
                                    "Item": {
                                        "pk": {"S": "new_item"},
                                        "data": {"S": "value"},
                                    },
                                }
                            },
                            {
                                "ConditionCheck": {
                                    "TableName": table_name,
                                    "Key": {"pk": {"S": "nonexistent"}},
                                    "ConditionExpression": "attribute_exists(pk)",
                                }
                            },
                        ]
                    )

                # new_item should NOT exist (rolled back)
                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "new_item"}},
                )
                assert "Item" not in response
            finally:
                client.delete_table(TableName=table_name)

    def test_transact_write_delete(
        self, config: AlternatorConfig, table_name: str
    ) -> None:
        """Test transactional delete of items."""
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
                # Pre-populate
                client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "to_delete"}, "data": {"S": "bye"}},
                )

                client.transact_write_items(
                    TransactItems=[
                        {
                            "Delete": {
                                "TableName": table_name,
                                "Key": {"pk": {"S": "to_delete"}},
                            }
                        },
                        {
                            "Put": {
                                "TableName": table_name,
                                "Item": {
                                    "pk": {"S": "replacement"},
                                    "data": {"S": "hello"},
                                },
                            }
                        },
                    ]
                )

                # Deleted item should be gone
                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "to_delete"}},
                )
                assert "Item" not in response

                # Replacement item should exist
                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "replacement"}},
                )
                assert response["Item"]["data"]["S"] == "hello"
            finally:
                client.delete_table(TableName=table_name)


class TestTransactGetItems:
    """Test TransactGetItems operations through the load-balanced client."""

    def test_transact_get_multiple_items(
        self, config: AlternatorConfig, table_name: str
    ) -> None:
        """Test transactional get of multiple items."""
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
                # Pre-populate items
                for i in range(3):
                    client.put_item(
                        TableName=table_name,
                        Item={
                            "pk": {"S": f"item_{i}"},
                            "data": {"S": f"value_{i}"},
                        },
                    )

                response = client.transact_get_items(
                    TransactItems=[
                        {
                            "Get": {
                                "TableName": table_name,
                                "Key": {"pk": {"S": f"item_{i}"}},
                            }
                        }
                        for i in range(3)
                    ]
                )

                items = response["Responses"]
                assert len(items) == 3
                for i, resp in enumerate(items):
                    assert resp["Item"]["pk"]["S"] == f"item_{i}"
                    assert resp["Item"]["data"]["S"] == f"value_{i}"
            finally:
                client.delete_table(TableName=table_name)

    def test_transact_get_with_missing_items(
        self, config: AlternatorConfig, table_name: str
    ) -> None:
        """Test transactional get when some items don't exist."""
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
                # Only put one item
                client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "exists"}, "data": {"S": "here"}},
                )

                response = client.transact_get_items(
                    TransactItems=[
                        {
                            "Get": {
                                "TableName": table_name,
                                "Key": {"pk": {"S": "exists"}},
                            }
                        },
                        {
                            "Get": {
                                "TableName": table_name,
                                "Key": {"pk": {"S": "missing"}},
                            }
                        },
                    ]
                )

                items = response["Responses"]
                assert len(items) == 2
                assert items[0]["Item"]["data"]["S"] == "here"
                assert items[1] == {}  # Missing item returns empty dict
            finally:
                client.delete_table(TableName=table_name)
