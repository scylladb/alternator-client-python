"""Integration tests for tables with composite keys (HASH + RANGE).

These tests require a running Scylla cluster with Alternator enabled.
Start a local cluster with: make scylla-start

"""

import uuid
from typing import Any

import pytest

from alternator import (
    Config,
    KeyRouteAffinityConfig,
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
    return f"test_composite_{uuid.uuid4().hex[:8]}"


def _create_composite_table(client: Any, table_name: str) -> None:  # noqa: ANN401 -- generated boto3 client type is unavailable in integration tests
    """Helper to create a table with HASH + RANGE key."""
    client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = client.get_waiter("table_exists")
    waiter.wait(TableName=table_name)


class TestCompositeKeyBasicOperations:
    """Test basic CRUD with composite (HASH + RANGE) keys."""

    def test_put_and_get_item(self, config: Config, table_name: str) -> None:
        """Test put and get with composite key."""
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_composite_table(client, table_name)
            try:
                client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": "user_1"},
                        "sk": {"S": "order_100"},
                        "total": {"N": "42.50"},
                    },
                )

                response = client.get_item(
                    TableName=table_name,
                    Key={
                        "pk": {"S": "user_1"},
                        "sk": {"S": "order_100"},
                    },
                )

                assert response["Item"]["pk"]["S"] == "user_1"
                assert response["Item"]["sk"]["S"] == "order_100"
                # ScyllaDB may strip trailing zeros from decimals (e.g. "42.50" → "42.5")
                assert response["Item"]["total"]["N"] in ("42.50", "42.5")
            finally:
                client.delete_table(TableName=table_name)

    def test_multiple_range_keys_same_partition(
        self, config: Config, table_name: str
    ) -> None:
        """Test multiple items with same HASH key but different RANGE keys."""
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_composite_table(client, table_name)
            try:
                for i in range(5):
                    client.put_item(
                        TableName=table_name,
                        Item={
                            "pk": {"S": "user_1"},
                            "sk": {"S": f"order_{i}"},
                            "data": {"S": f"value_{i}"},
                        },
                    )

                # Each item should be independently retrievable
                for i in range(5):
                    response = client.get_item(
                        TableName=table_name,
                        Key={
                            "pk": {"S": "user_1"},
                            "sk": {"S": f"order_{i}"},
                        },
                    )
                    assert response["Item"]["data"]["S"] == f"value_{i}"
            finally:
                client.delete_table(TableName=table_name)

    def test_update_item_composite_key(self, config: Config, table_name: str) -> None:
        """Test updating an item with composite key."""
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_composite_table(client, table_name)
            try:
                client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": "user_1"},
                        "sk": {"S": "profile"},
                        "name": {"S": "Alice"},
                    },
                )

                client.update_item(
                    TableName=table_name,
                    Key={
                        "pk": {"S": "user_1"},
                        "sk": {"S": "profile"},
                    },
                    UpdateExpression="SET #n = :v",
                    ExpressionAttributeNames={"#n": "name"},
                    ExpressionAttributeValues={":v": {"S": "Bob"}},
                )

                response = client.get_item(
                    TableName=table_name,
                    Key={
                        "pk": {"S": "user_1"},
                        "sk": {"S": "profile"},
                    },
                )
                assert response["Item"]["name"]["S"] == "Bob"
            finally:
                client.delete_table(TableName=table_name)

    def test_delete_item_composite_key(self, config: Config, table_name: str) -> None:
        """Test deleting an item with composite key."""
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_composite_table(client, table_name)
            try:
                client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": "user_1"},
                        "sk": {"S": "to_delete"},
                        "data": {"S": "temp"},
                    },
                )

                client.delete_item(
                    TableName=table_name,
                    Key={
                        "pk": {"S": "user_1"},
                        "sk": {"S": "to_delete"},
                    },
                )

                response = client.get_item(
                    TableName=table_name,
                    Key={
                        "pk": {"S": "user_1"},
                        "sk": {"S": "to_delete"},
                    },
                )
                assert "Item" not in response
            finally:
                client.delete_table(TableName=table_name)


class TestCompositeKeyQuery:
    """Test query operations with composite keys."""

    def test_query_by_partition_key(self, config: Config, table_name: str) -> None:
        """Test querying all range keys for a given partition key."""
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_composite_table(client, table_name)
            try:
                # Insert items for two different partition keys
                for user in ("user_a", "user_b"):
                    for i in range(3):
                        client.put_item(
                            TableName=table_name,
                            Item={
                                "pk": {"S": user},
                                "sk": {"S": f"item_{i}"},
                                "data": {"S": f"{user}_{i}"},
                            },
                        )

                # Query only user_a's items
                response = client.query(
                    TableName=table_name,
                    KeyConditionExpression="pk = :pk",
                    ExpressionAttributeValues={":pk": {"S": "user_a"}},
                )

                assert response["Count"] == 3
                items = response["Items"]
                assert all(item["pk"]["S"] == "user_a" for item in items)
            finally:
                client.delete_table(TableName=table_name)

    def test_query_with_range_key_condition(
        self, config: Config, table_name: str
    ) -> None:
        """Test querying with both partition and range key conditions."""
        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_composite_table(client, table_name)
            try:
                for i in range(5):
                    client.put_item(
                        TableName=table_name,
                        Item={
                            "pk": {"S": "user_1"},
                            "sk": {"S": f"event_{i:03d}"},
                            "data": {"S": f"data_{i}"},
                        },
                    )

                # Query with range key condition (begins_with)
                response = client.query(
                    TableName=table_name,
                    KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
                    ExpressionAttributeValues={
                        ":pk": {"S": "user_1"},
                        ":prefix": {"S": "event_00"},
                    },
                )

                # Should match event_000, event_001, ..., event_009
                # We only inserted event_000 through event_004
                assert response["Count"] == 5
            finally:
                client.delete_table(TableName=table_name)


class TestCompositeKeyAffinity:
    """Test key affinity with composite keys.

    Key affinity should route based on the HASH key only,
    ignoring the RANGE key.
    """

    def test_affinity_uses_hash_key_only(self, table_name: str) -> None:
        """Test that affinity routing works with composite keys."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            key_affinity=KeyRouteAffinityConfig(
                mode=KeyRouteAffinityMode.ANY_WRITE,
                table_pk_attributes={table_name: "pk"},
            ),
        )

        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_composite_table(client, table_name)
            try:
                # Write multiple items with same HASH key, different RANGE keys
                for i in range(5):
                    client.put_item(
                        TableName=table_name,
                        Item={
                            "pk": {"S": "same_partition"},
                            "sk": {"S": f"range_{i}"},
                            "data": {"S": f"value_{i}"},
                        },
                    )

                # Verify all items
                for i in range(5):
                    response = client.get_item(
                        TableName=table_name,
                        Key={
                            "pk": {"S": "same_partition"},
                            "sk": {"S": f"range_{i}"},
                        },
                    )
                    assert response["Item"]["data"]["S"] == f"value_{i}"
            finally:
                client.delete_table(TableName=table_name)

    def test_affinity_auto_discover_pk(self, table_name: str) -> None:
        """Test auto-discovery of partition key for composite key table."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            key_affinity=KeyRouteAffinityConfig(mode=KeyRouteAffinityMode.ANY_WRITE),
        )

        with alternator_client("dynamodb", cluster_config=config) as client:
            _create_composite_table(client, table_name)
            try:
                # Should auto-discover that "pk" is the HASH key
                client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": "auto_key"},
                        "sk": {"S": "sort_1"},
                        "data": {"S": "test"},
                    },
                )

                response = client.get_item(
                    TableName=table_name,
                    Key={
                        "pk": {"S": "auto_key"},
                        "sk": {"S": "sort_1"},
                    },
                )
                assert response["Item"]["data"]["S"] == "test"
            finally:
                client.delete_table(TableName=table_name)
