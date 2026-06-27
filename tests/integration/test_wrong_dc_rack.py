"""Integration tests for wrong DC/rack error handling and fallback.

These tests verify that the client gracefully handles incorrect datacenter
and rack configurations when broader fallback scopes are configured.

These tests require a running Scylla cluster with Alternator enabled.
Start a local cluster with: make scylla-start
"""

import pytest

from alternator import (
    AlternatorClient,
    AlternatorConfigBuilder,
    ClusterScope,
    Config,
    DatacenterScope,
    Helper,
    NoNodesAvailableError,
    RackScope,
)
from alternator.exceptions import ConfigurationError
from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


class TestWrongDatacenter:
    """Test wrong datacenter detection and fallback."""

    def test_wrong_datacenter_falls_back_to_cluster(self) -> None:
        """Explicit fallback lets a non-existent datacenter use cluster scope.

        The /localnodes?dc=nonexistent endpoint returns empty results,
        causing the manager to fall back to ClusterScope.
        """
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            routing_scope=DatacenterScope(
                "nonexistent_dc_12345", fallback=ClusterScope()
            ),
        )

        assert isinstance(config.routing_scope, DatacenterScope)

        # Should still work because fallback to cluster scope is explicit.
        with AlternatorClient(config) as client:
            response = client.list_tables()
            assert "TableNames" in response

    def test_wrong_datacenter_still_handles_operations(self) -> None:
        """After fallback, all operations should work normally."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            routing_scope=DatacenterScope("bad_dc", fallback=ClusterScope()),
        )

        with AlternatorClient(config) as client:
            # Multiple operations should all succeed after fallback
            for _ in range(5):
                response = client.list_tables()
                assert "TableNames" in response

    def test_wrong_datacenter_without_fallback_fails(self) -> None:
        """Default datacenter-only routing fails when that datacenter is absent."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            routing_scope=DatacenterScope("bad_dc"),
        )

        with pytest.raises(NoNodesAvailableError), AlternatorClient(config):
            pass


class TestWrongRack:
    """Test wrong rack detection and fallback."""

    def test_wrong_rack_falls_back_to_datacenter(self) -> None:
        """Explicit fallback lets a non-existent rack use broader scopes.

        Fallback chain: Rack → Datacenter → Cluster
        """
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            routing_scope=RackScope(
                "datacenter1",
                "nonexistent_rack_12345",
                fallback=DatacenterScope("datacenter1", fallback=ClusterScope()),
            ),
        )

        assert isinstance(config.routing_scope, RackScope)

        # Should still work because fallback through datacenter to cluster is explicit.
        with AlternatorClient(config) as client:
            response = client.list_tables()
            assert "TableNames" in response

    def test_wrong_rack_and_datacenter_falls_back_to_cluster(self) -> None:
        """Explicit fallback lets wrong rack and datacenter use cluster scope."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            routing_scope=RackScope(
                "bad_dc",
                "bad_rack",
                fallback=DatacenterScope("bad_dc", fallback=ClusterScope()),
            ),
        )

        assert isinstance(config.routing_scope, RackScope)

        with AlternatorClient(config) as client:
            response = client.list_tables()
            assert "TableNames" in response

    def test_wrong_rack_still_handles_operations(self) -> None:
        """After rack fallback, all operations should work normally."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            routing_scope=RackScope(
                "datacenter1",
                "bad_rack",
                fallback=DatacenterScope("datacenter1", fallback=ClusterScope()),
            ),
        )

        with AlternatorClient(config) as client:
            for _ in range(5):
                response = client.list_tables()
                assert "TableNames" in response


class TestRackDatacenterFeatureSupport:
    """Test that the /localnodes endpoint supports rack/datacenter filtering."""

    def test_datacenter_scope_query_format(self) -> None:
        """Verify DatacenterScope produces correct query string."""
        scope = DatacenterScope(datacenter="dc1")
        assert scope.get_localnodes_query() == "dc=dc1"

    def test_rack_scope_query_format(self) -> None:
        """Verify RackScope produces correct query string."""
        scope = RackScope(datacenter="dc1", rack="rack1")
        assert scope.get_localnodes_query() == "dc=dc1&rack=rack1"

    def test_datacenter_scope_with_special_characters(self) -> None:
        """Verify special characters in DC name are URL-encoded."""
        scope = DatacenterScope(datacenter="dc 1/test")
        query = scope.get_localnodes_query()
        assert "dc%201%2Ftest" in query

    def test_cluster_scope_returns_nodes(self) -> None:
        """Verify cluster scope returns nodes from the running cluster."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .build()
        )

        with AlternatorClient(config) as client:
            response = client.list_tables()
            assert "TableNames" in response

    def test_correct_datacenter_works(self) -> None:
        """Test that the correct datacenter name works without fallback.

        The default Scylla docker-compose uses 'datacenter1'.
        """
        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .with_datacenter("datacenter1")
            .build()
        )

        with AlternatorClient(config) as client:
            response = client.list_tables()
            assert "TableNames" in response

    def test_helper_validates_correct_datacenter(self) -> None:
        """Helper validation succeeds for a known datacenter."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            routing_scope=DatacenterScope("datacenter1", fallback=None),
        )

        assert Helper(config).check_rack_and_datacenter_set_correctly()

    def test_helper_validation_rejects_wrong_datacenter(self) -> None:
        """Helper validation raises a clear error for an absent datacenter."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            routing_scope=DatacenterScope("bad_dc", fallback=None),
        )

        with pytest.raises(ConfigurationError, match="No nodes found"):
            Helper(config).check_rack_and_datacenter_set_correctly()
