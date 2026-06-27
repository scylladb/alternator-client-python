"""Integration tests for synchronous client.

These tests require a running Scylla cluster with Alternator enabled.
Start a local cluster with: make scylla-start

"""

import uuid
from collections.abc import Callable

import pytest

from alternator import (
    Auth,
    CompressionAlgorithm,
    Config,
    HeaderOptimizationConfig,
    KeyRouteAffinityConfig,
    KeyRouteAffinityMode,
    RequestCompressionConfig,
)
from alternator import (
    client as alternator_client,
)
from tests.integration import (
    SCYLLA_HOST,
    SCYLLA_HTTPS_PORT,
    SCYLLA_PORT,
    SKIP_INTEGRATION,
)

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
    return f"test_table_{uuid.uuid4().hex[:8]}"


class TestBasicOperations:
    """Test basic DynamoDB operations through the load-balanced client."""

    def test_list_tables(self, config: Config) -> None:
        """Test listing tables."""
        with alternator_client("dynamodb", cluster_config=config) as client:
            response = client.list_tables()
            assert "TableNames" in response
            assert isinstance(response["TableNames"], list)

    def test_create_and_delete_table(self, config: Config, table_name: str) -> None:
        """Test creating and deleting a table."""
        with alternator_client("dynamodb", cluster_config=config) as client:
            # Create table
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

            # Wait for table to be active
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            # Verify table exists
            response = client.describe_table(TableName=table_name)
            assert response["Table"]["TableStatus"] == "ACTIVE"

            # Delete table
            client.delete_table(TableName=table_name)

            # Wait for deletion
            waiter = client.get_waiter("table_not_exists")
            waiter.wait(TableName=table_name)

    def test_put_and_get_item(self, config: Config, table_name: str) -> None:
        """Test putting and getting an item."""
        with alternator_client("dynamodb", cluster_config=config) as client:
            # Create table
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                # Put item
                client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": "test_key"},
                        "data": {"S": "test_value"},
                        "number": {"N": "42"},
                    },
                )

                # Get item
                response = client.get_item(
                    TableName=table_name, Key={"pk": {"S": "test_key"}}
                )

                assert "Item" in response
                assert response["Item"]["pk"]["S"] == "test_key"
                assert response["Item"]["data"]["S"] == "test_value"
                assert response["Item"]["number"]["N"] == "42"
            finally:
                client.delete_table(TableName=table_name)


class TestLoadBalancing:
    """Test load balancing functionality."""

    def test_requests_distributed(self, config: Config) -> None:
        """Test that requests are distributed across nodes."""
        with alternator_client("dynamodb", cluster_config=config) as client:
            # Make multiple requests
            for _ in range(10):
                client.list_tables()
            # If we get here without errors, load balancing is working


class TestCompression:
    """Test request compression.

    Note: Gzip request compression requires ScyllaDB 2026.1.0+
    See: https://github.com/scylladb/scylladb/pull/27080
    """

    def test_compression_with_large_item(
        self, table_name: str, skip_if_scylla_version_below: Callable[..., None]
    ) -> None:
        """Test compression is applied for large items."""
        from tests.integration.scylla_version import ScyllaVersion

        skip_if_scylla_version_below(
            ScyllaVersion(2026, 1, 0), "gzip request compression"
        )

        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            request_compression=RequestCompressionConfig(
                algorithm=CompressionAlgorithm.GZIP,
                min_size_bytes=100,
            ),
        )

        with alternator_client("dynamodb", cluster_config=config) as client:
            # Create table
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                # Put a large item (should be compressed)
                large_data = "x" * 10000
                client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": "large_key"},
                        "data": {"S": large_data},
                    },
                )

                # Verify item was stored correctly
                response = client.get_item(
                    TableName=table_name, Key={"pk": {"S": "large_key"}}
                )
                assert response["Item"]["data"]["S"] == large_data
            finally:
                client.delete_table(TableName=table_name)


class TestKeyAffinity:
    """Test key affinity routing."""

    def test_rmw_affinity(self, table_name: str) -> None:
        """Test RMW operations use affinity routing."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            key_affinity=KeyRouteAffinityConfig(
                mode=KeyRouteAffinityMode.RMW,
                table_pk_attributes={table_name: "pk"},
            ),
        )

        with alternator_client("dynamodb", cluster_config=config) as client:
            # Create table
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                # Insert initial item
                client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "counter"}, "value": {"N": "0"}},
                )

                # Perform conditional updates (RMW operations)
                for _ in range(5):
                    client.update_item(
                        TableName=table_name,
                        Key={"pk": {"S": "counter"}},
                        UpdateExpression="SET #v = #v + :inc",
                        ConditionExpression="attribute_exists(pk)",
                        ExpressionAttributeNames={"#v": "value"},
                        ExpressionAttributeValues={":inc": {"N": "1"}},
                    )

                # Verify final value
                response = client.get_item(
                    TableName=table_name, Key={"pk": {"S": "counter"}}
                )
                assert response["Item"]["value"]["N"] == "5"
            finally:
                client.delete_table(TableName=table_name)


class TestClientContextManagement:
    """Test client context-manager lifecycle management."""

    def test_context_manager_opens_and_closes(self, config: Config) -> None:
        """Test creating a client through the public context manager."""
        with alternator_client("dynamodb", cluster_config=config) as client:
            response = client.list_tables()
            assert "TableNames" in response

    def test_context_close_is_idempotent(self, config: Config) -> None:
        """Test that closing a client context twice is safe."""
        ctx = alternator_client("dynamodb", cluster_config=config)
        with ctx as client:
            assert "TableNames" in client.list_tables()
        ctx.close()


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_invalid_table_raises(self, config: Config) -> None:
        """Test that accessing invalid table raises appropriate error."""
        with (
            alternator_client("dynamodb", cluster_config=config) as client,
            pytest.raises(client.exceptions.ResourceNotFoundException),
        ):
            client.describe_table(TableName="nonexistent_table_12345")


class TestKeyAffinityDistribution:
    """Test that different keys route to different nodes."""

    def test_different_keys_route_differently(self, table_name: str) -> None:
        """Test different partition keys probabilistically route to different nodes.

        With multiple nodes, different keys should hash to different nodes.
        This test verifies that the affinity routing actually uses the key
        for node selection, not just round-robin.
        """
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            key_affinity=KeyRouteAffinityConfig(
                mode=KeyRouteAffinityMode.ANY_WRITE,
                table_pk_attributes={table_name: "pk"},
            ),
        )

        with alternator_client("dynamodb", cluster_config=config) as client:
            # Create table
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                # Write items with different keys
                # The same key should always route to the same node (deterministic)
                for i in range(10):
                    client.put_item(
                        TableName=table_name,
                        Item={
                            "pk": {"S": f"key_{i}"},
                            "data": {"S": f"value_{i}"},
                        },
                    )

                # Verify all items were written correctly
                for i in range(10):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"key_{i}"}},
                    )
                    assert response["Item"]["data"]["S"] == f"value_{i}"
            finally:
                client.delete_table(TableName=table_name)


class TestPartitionKeyAutoDiscovery:
    """Test auto-discovery of partition key via DescribeTable."""

    def test_auto_discover_pk_name(self, table_name: str) -> None:
        """Test that partition key name is auto-discovered via DescribeTable."""
        # Configure affinity WITHOUT pre-defined table_pk_map
        # The client should discover the PK name automatically
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            key_affinity=KeyRouteAffinityConfig(mode=KeyRouteAffinityMode.ANY_WRITE),
        )

        with alternator_client("dynamodb", cluster_config=config) as client:
            # Create table with custom PK name
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "my_custom_pk", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "my_custom_pk", "AttributeType": "S"}
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                # Write item - this should trigger auto-discovery
                client.put_item(
                    TableName=table_name,
                    Item={
                        "my_custom_pk": {"S": "test_key"},
                        "data": {"S": "test_value"},
                    },
                )

                # Verify the item was stored correctly
                response = client.get_item(
                    TableName=table_name,
                    Key={"my_custom_pk": {"S": "test_key"}},
                )
                assert response["Item"]["data"]["S"] == "test_value"

                # The second write should use cached PK name
                client.put_item(
                    TableName=table_name,
                    Item={
                        "my_custom_pk": {"S": "test_key_2"},
                        "data": {"S": "another_value"},
                    },
                )

                response = client.get_item(
                    TableName=table_name,
                    Key={"my_custom_pk": {"S": "test_key_2"}},
                )
                assert response["Item"]["data"]["S"] == "another_value"
            finally:
                client.delete_table(TableName=table_name)


class TestHeaderOptimization:
    """Test header filtering/optimization."""

    def test_header_optimized_requests_work(self, table_name: str) -> None:
        """Test that requests with header optimization work correctly."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            header_optimization=HeaderOptimizationConfig(enabled=True),
        )

        with alternator_client("dynamodb", cluster_config=config) as client:
            # Create table
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                # Perform various operations with header optimization
                client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": "header_test"},
                        "data": {"S": "value"},
                    },
                )

                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "header_test"}},
                )
                assert response["Item"]["data"]["S"] == "value"

                client.update_item(
                    TableName=table_name,
                    Key={"pk": {"S": "header_test"}},
                    UpdateExpression="SET #d = :v",
                    ExpressionAttributeNames={"#d": "data"},
                    ExpressionAttributeValues={":v": {"S": "updated"}},
                )

                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": "header_test"}},
                )
                assert response["Item"]["data"]["S"] == "updated"
            finally:
                client.delete_table(TableName=table_name)

    def test_header_optimization_with_custom_whitelist(self, table_name: str) -> None:
        """Test header optimization with custom whitelist."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            header_optimization=HeaderOptimizationConfig(
                enabled=True,
                whitelist=frozenset({"X-Custom-Header"}),
            ),
        )

        with alternator_client("dynamodb", cluster_config=config) as client:
            # Basic operation should work
            response = client.list_tables()
            assert "TableNames" in response

    def test_with_credentials_and_header_optimization(self) -> None:
        """Test header optimization keeps auth headers when credentials are provided."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            header_optimization=HeaderOptimizationConfig(enabled=True),
        )

        with alternator_client(
            "dynamodb",
            cluster_config=config,
            auth=Auth.static_credentials("alternator", "secret"),
            region_name="us-east-1",
        ) as client:
            response = client.list_tables()
            assert "TableNames" in response


class TestRoutingScopes:
    """Test datacenter and rack routing scopes."""

    def test_datacenter_scope_works(self) -> None:
        """Test datacenter-scoped routing.

        The default Scylla docker-compose uses 'datacenter1'.
        """
        from alternator import DatacenterScope

        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            routing_scope=DatacenterScope("datacenter1"),
        )

        # Verify the config has the correct scope
        assert isinstance(config.routing_scope, DatacenterScope)

        with alternator_client("dynamodb", cluster_config=config) as client:
            response = client.list_tables()
            assert "TableNames" in response

    def test_rack_scope_works(self) -> None:
        """Test rack-scoped routing.

        This test uses an explicit fallback chain because the local fixture may
        not expose rack1.
        """
        from alternator import ClusterScope, DatacenterScope, RackScope

        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            routing_scope=RackScope(
                "datacenter1",
                "rack1",
                fallback=DatacenterScope("datacenter1", fallback=ClusterScope()),
            ),
        )

        # Verify the config has the correct scope
        assert isinstance(config.routing_scope, RackScope)

        with alternator_client("dynamodb", cluster_config=config) as client:
            response = client.list_tables()
            assert "TableNames" in response


class TestTLSConfiguration:
    """Test TLS configuration options.

    These tests use the self-signed certificate generated by `make scylla-start`
    and the HTTPS port (default 9999).
    """

    def test_tls_custom_ca(self) -> None:
        """Test TLS with custom CA certificate (self-signed cert acts as CA).

        Note: TLS affects node discovery (/localnodes). For boto3
        connections, we also pass ``verify=<ca_path>`` so botocore uses
        the same CA bundle.
        """
        from pathlib import Path

        from alternator import TLS

        ca_path = Path(__file__).resolve().parents[1] / "scylla" / "db.crt"
        if not ca_path.exists():
            pytest.skip("Self-signed certificate not found (run 'make scylla-start')")

        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_HTTPS_PORT,
            scheme="https",
            tls=TLS.with_custom_ca(ca_path),
        )

        with alternator_client(
            "dynamodb", cluster_config=config, verify=str(ca_path)
        ) as client:
            response = client.list_tables()
            assert "TableNames" in response

    def test_tls_trust_all(self) -> None:
        """Test TLS with trust-all mode (insecure, for testing only).

        Note: TLS.trust_all() affects node discovery. For boto3
        connections, we also pass ``verify=False`` so botocore skips
        certificate verification.
        """
        from alternator import TLS

        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_HTTPS_PORT,
            scheme="https",
            tls=TLS.trust_all(),
        )

        with alternator_client(
            "dynamodb", cluster_config=config, verify=False
        ) as client:
            response = client.list_tables()
            assert "TableNames" in response

    def test_tls_system_default_rejects_self_signed(self) -> None:
        """Test that system CA store rejects self-signed certificates.

        The self-signed cert is not in the system CA store, so this should fail.
        """
        from botocore.exceptions import SSLError

        from alternator import TLS

        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_HTTPS_PORT,
            scheme="https",
            tls=TLS.system_default(),
        )

        with (
            pytest.raises((ConnectionError, SSLError, Exception)),
            alternator_client("dynamodb", cluster_config=config) as client,
        ):
            client.list_tables()
