"""Integration tests for asynchronous client.

These tests require a running Scylla cluster with Alternator enabled.
Start a local cluster with: make scylla-start

"""

import asyncio
import uuid
from collections.abc import Callable

import pytest

from alternator import Config
from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
    pytest.mark.asyncio,
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
    return f"test_async_{uuid.uuid4().hex[:8]}"


class TestAsyncBasicOperations:
    """Test basic async DynamoDB operations."""

    async def test_list_tables(self, config: Config) -> None:
        """Test listing tables asynchronously."""
        from alternator.async_client import AsyncSession

        async with AsyncSession(config) as session:
            client = await session.client("dynamodb")
            response = await client.list_tables()
            assert "TableNames" in response
            assert isinstance(response["TableNames"], list)

    async def test_put_and_get_item(self, config: Config, table_name: str) -> None:
        """Test putting and getting an item asynchronously."""
        from alternator.async_client import AsyncSession

        async with AsyncSession(config) as session:
            client = await session.client("dynamodb")
            # Create table
            await client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

            # Wait for table to be active
            while True:
                response = await client.describe_table(TableName=table_name)
                if response["Table"]["TableStatus"] == "ACTIVE":
                    break
                await asyncio.sleep(1)

            try:
                # Put item
                await client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": "async_key"},
                        "data": {"S": "async_value"},
                    },
                )

                # Get item
                response = await client.get_item(
                    TableName=table_name, Key={"pk": {"S": "async_key"}}
                )

                assert "Item" in response
                assert response["Item"]["pk"]["S"] == "async_key"
                assert response["Item"]["data"]["S"] == "async_value"
            finally:
                await client.delete_table(TableName=table_name)


class TestAsyncConcurrency:
    """Test concurrent async operations."""

    async def test_concurrent_writes(self, config: Config, table_name: str) -> None:
        """Test concurrent write operations."""
        from alternator.async_client import AsyncSession

        async with AsyncSession(config) as session:
            client = await session.client("dynamodb")
            # Create table
            await client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

            while True:
                response = await client.describe_table(TableName=table_name)
                if response["Table"]["TableStatus"] == "ACTIVE":
                    break
                await asyncio.sleep(1)

            try:
                # Write items concurrently
                tasks = [
                    client.put_item(
                        TableName=table_name,
                        Item={
                            "pk": {"S": f"concurrent_{i}"},
                            "data": {"S": f"value_{i}"},
                        },
                    )
                    for i in range(20)
                ]
                await asyncio.gather(*tasks)

                # Read items concurrently
                read_tasks = [
                    client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"concurrent_{i}"}},
                    )
                    for i in range(20)
                ]
                responses = await asyncio.gather(*read_tasks)

                # Verify all items exist
                for i, response in enumerate(responses):
                    assert "Item" in response
                    assert response["Item"]["data"]["S"] == f"value_{i}"
            finally:
                await client.delete_table(TableName=table_name)

    async def test_high_concurrency(self, config: Config) -> None:
        """Test high concurrency list_tables operations."""
        from alternator.async_client import AsyncSession

        async with AsyncSession(config) as session:
            client = await session.client("dynamodb")
            # Run many concurrent requests
            tasks = [client.list_tables() for _ in range(100)]
            responses = await asyncio.gather(*tasks)

            # All should succeed
            for response in responses:
                assert "TableNames" in response


class TestAsyncKeyAffinity:
    """Test async client with key affinity routing."""

    async def test_async_rmw_affinity(self, table_name: str) -> None:
        """Test RMW operations use affinity routing with async client."""
        from alternator import AlternatorConfigBuilder, KeyRouteAffinityMode
        from alternator.async_client import AsyncSession

        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .with_key_affinity(
                KeyRouteAffinityMode.RMW,
                table_pk_map={table_name: "pk"},
            )
            .build()
        )

        async with AsyncSession(config) as session:
            client = await session.client("dynamodb")
            # Create table
            await client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

            while True:
                response = await client.describe_table(TableName=table_name)
                if response["Table"]["TableStatus"] == "ACTIVE":
                    break
                await asyncio.sleep(1)

            try:
                # Insert initial item
                await client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "async_counter"}, "value": {"N": "0"}},
                )

                # Perform conditional updates (RMW operations) concurrently
                # These should all route to the same node for the same key
                async def increment() -> None:
                    await client.update_item(
                        TableName=table_name,
                        Key={"pk": {"S": "async_counter"}},
                        UpdateExpression="SET #v = #v + :inc",
                        ConditionExpression="attribute_exists(pk)",
                        ExpressionAttributeNames={"#v": "value"},
                        ExpressionAttributeValues={":inc": {"N": "1"}},
                    )

                # Run increments sequentially to avoid race conditions
                for _ in range(5):
                    await increment()

                # Verify final value
                response = await client.get_item(
                    TableName=table_name, Key={"pk": {"S": "async_counter"}}
                )
                assert response["Item"]["value"]["N"] == "5"
            finally:
                await client.delete_table(TableName=table_name)

    async def test_async_any_write_affinity(self, table_name: str) -> None:
        """Test ANY_WRITE mode routes all writes through affinity."""
        from alternator import AlternatorConfigBuilder, KeyRouteAffinityMode
        from alternator.async_client import AsyncSession

        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .with_key_affinity(
                KeyRouteAffinityMode.ANY_WRITE,
                table_pk_map={table_name: "pk"},
            )
            .build()
        )

        async with AsyncSession(config) as session:
            client = await session.client("dynamodb")
            # Create table
            await client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

            while True:
                response = await client.describe_table(TableName=table_name)
                if response["Table"]["TableStatus"] == "ACTIVE":
                    break
                await asyncio.sleep(1)

            try:
                # Write items with different keys concurrently
                tasks = [
                    client.put_item(
                        TableName=table_name,
                        Item={
                            "pk": {"S": f"async_key_{i}"},
                            "data": {"S": f"async_value_{i}"},
                        },
                    )
                    for i in range(10)
                ]
                await asyncio.gather(*tasks)

                # Read all items back
                read_tasks = [
                    client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"async_key_{i}"}},
                    )
                    for i in range(10)
                ]
                responses = await asyncio.gather(*read_tasks)

                # Verify all items
                for i, response in enumerate(responses):
                    assert "Item" in response
                    assert response["Item"]["data"]["S"] == f"async_value_{i}"
            finally:
                await client.delete_table(TableName=table_name)

    async def test_async_pk_auto_discovery(self, table_name: str) -> None:
        """Test async auto-discovery of partition key via DescribeTable."""
        from alternator import AlternatorConfigBuilder, KeyRouteAffinityMode
        from alternator.async_client import AsyncSession

        # Configure affinity WITHOUT pre-defined table_pk_map
        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .with_key_affinity(KeyRouteAffinityMode.ANY_WRITE)
            .build()
        )

        async with AsyncSession(config) as session:
            client = await session.client("dynamodb")
            # Create table with custom PK name
            await client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "custom_async_pk", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "custom_async_pk", "AttributeType": "S"}
                ],
                BillingMode="PAY_PER_REQUEST",
            )

            while True:
                response = await client.describe_table(TableName=table_name)
                if response["Table"]["TableStatus"] == "ACTIVE":
                    break
                await asyncio.sleep(1)

            try:
                # Write item - triggers auto-discovery
                await client.put_item(
                    TableName=table_name,
                    Item={
                        "custom_async_pk": {"S": "async_test"},
                        "data": {"S": "async_value"},
                    },
                )

                # Verify item stored correctly
                response = await client.get_item(
                    TableName=table_name,
                    Key={"custom_async_pk": {"S": "async_test"}},
                )
                assert response["Item"]["data"]["S"] == "async_value"
            finally:
                await client.delete_table(TableName=table_name)


class TestAsyncCompression:
    """Test async client with compression."""

    async def test_async_compression(
        self,
        table_name: str,
        skip_if_scylla_version_below: Callable[..., None],
    ) -> None:
        """Test compression with async client."""
        from alternator import AlternatorConfigBuilder, CompressionAlgorithm
        from alternator.async_client import AsyncSession
        from tests.integration.scylla_version import ScyllaVersion

        skip_if_scylla_version_below(
            ScyllaVersion(2026, 1, 0), "gzip request compression"
        )

        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .with_compression(CompressionAlgorithm.GZIP, min_size=100)
            .build()
        )

        async with AsyncSession(config) as session:
            client = await session.client("dynamodb")
            # Create table
            await client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

            while True:
                response = await client.describe_table(TableName=table_name)
                if response["Table"]["TableStatus"] == "ACTIVE":
                    break
                await asyncio.sleep(1)

            try:
                # Put a large item (should be compressed)
                large_data = "y" * 10000
                await client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": "async_large_key"},
                        "data": {"S": large_data},
                    },
                )

                # Verify item was stored correctly
                response = await client.get_item(
                    TableName=table_name, Key={"pk": {"S": "async_large_key"}}
                )
                assert response["Item"]["data"]["S"] == large_data
            finally:
                await client.delete_table(TableName=table_name)


class TestAsyncHeaderOptimization:
    """Test async client with header optimization."""

    async def test_async_header_optimization(self, table_name: str) -> None:
        """Test header optimization with async client."""
        from alternator import AlternatorConfigBuilder
        from alternator.async_client import AsyncSession

        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .with_header_optimization()
            .build()
        )

        async with AsyncSession(config) as session:
            client = await session.client("dynamodb")
            # Create table
            await client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

            while True:
                response = await client.describe_table(TableName=table_name)
                if response["Table"]["TableStatus"] == "ACTIVE":
                    break
                await asyncio.sleep(1)

            try:
                # Perform operations with header optimization
                await client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": "async_header_test"},
                        "data": {"S": "value"},
                    },
                )

                response = await client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "async_header_test"}},
                )
                assert response["Item"]["data"]["S"] == "value"
            finally:
                await client.delete_table(TableName=table_name)
