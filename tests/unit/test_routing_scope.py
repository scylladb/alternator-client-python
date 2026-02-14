"""Tests for routing scope classes."""

from alternator.core.routing_scope import (
    ClusterScope,
    DatacenterScope,
    RackScope,
    RoutingScope,
)


class TestClusterScope:
    """Tests for ClusterScope."""

    def test_name(self) -> None:
        """Test scope name."""
        scope = ClusterScope()
        assert scope.name == "Cluster"

    def test_description(self) -> None:
        """Test scope description."""
        scope = ClusterScope()
        assert scope.description == "All nodes in cluster"

    def test_fallback_is_none(self) -> None:
        """Test ClusterScope has no fallback."""
        scope = ClusterScope()
        assert scope.fallback is None

    def test_query_string_is_empty(self) -> None:
        """Test query string is empty for cluster scope."""
        scope = ClusterScope()
        assert scope.get_localnodes_query() == ""


class TestDatacenterScope:
    """Tests for DatacenterScope."""

    def test_name(self) -> None:
        """Test scope name."""
        scope = DatacenterScope(datacenter="us-east-1")
        assert scope.name == "Datacenter"

    def test_description(self) -> None:
        """Test scope description includes datacenter."""
        scope = DatacenterScope(datacenter="us-east-1")
        assert scope.description == "Nodes in datacenter 'us-east-1'"

    def test_query_string(self) -> None:
        """Test query string format."""
        scope = DatacenterScope(datacenter="us-east-1")
        assert scope.get_localnodes_query() == "dc=us-east-1"

    def test_default_fallback_to_cluster(self) -> None:
        """Test default fallback is ClusterScope."""
        scope = DatacenterScope(datacenter="us-east-1")
        fallback = scope.fallback
        assert isinstance(fallback, ClusterScope)

    def test_custom_fallback(self) -> None:
        """Test custom fallback scope."""
        custom_fallback = ClusterScope()
        scope = DatacenterScope(datacenter="us-east-1", _fallback=custom_fallback)
        assert scope.fallback is custom_fallback


class TestRackScope:
    """Tests for RackScope."""

    def test_name(self) -> None:
        """Test scope name."""
        scope = RackScope(datacenter="us-east-1", rack="rack1")
        assert scope.name == "Rack"

    def test_description(self) -> None:
        """Test scope description includes dc and rack."""
        scope = RackScope(datacenter="us-east-1", rack="rack1")
        assert scope.description == "Nodes in rack 'rack1' of datacenter 'us-east-1'"

    def test_query_string(self) -> None:
        """Test query string format."""
        scope = RackScope(datacenter="us-east-1", rack="rack1")
        assert scope.get_localnodes_query() == "dc=us-east-1&rack=rack1"

    def test_default_fallback_to_datacenter(self) -> None:
        """Test default fallback is DatacenterScope."""
        scope = RackScope(datacenter="us-east-1", rack="rack1")
        fallback = scope.fallback
        assert isinstance(fallback, DatacenterScope)
        assert fallback.datacenter == "us-east-1"

    def test_custom_fallback(self) -> None:
        """Test custom fallback scope."""
        custom_fallback = ClusterScope()
        scope = RackScope(datacenter="dc1", rack="r1", _fallback=custom_fallback)
        assert scope.fallback is custom_fallback


class TestFallbackChain:
    """Tests for routing scope fallback chains."""

    def test_rack_fallback_chain(self) -> None:
        """Test full fallback chain: Rack -> Datacenter -> Cluster."""
        rack_scope = RackScope(datacenter="us-east-1", rack="rack1")

        # Rack -> Datacenter
        dc_scope = rack_scope.fallback
        assert isinstance(dc_scope, DatacenterScope)
        assert dc_scope.datacenter == "us-east-1"

        # Datacenter -> Cluster
        cluster_scope = dc_scope.fallback
        assert isinstance(cluster_scope, ClusterScope)

        # Cluster -> None
        assert cluster_scope.fallback is None

    def test_datacenter_fallback_chain(self) -> None:
        """Test fallback chain: Datacenter -> Cluster."""
        dc_scope = DatacenterScope(datacenter="eu-west-1")

        # Datacenter -> Cluster
        cluster_scope = dc_scope.fallback
        assert isinstance(cluster_scope, ClusterScope)

        # Cluster -> None
        assert cluster_scope.fallback is None

    def test_all_scopes_are_routing_scope(self) -> None:
        """Test all scope classes inherit from RoutingScope."""
        assert isinstance(ClusterScope(), RoutingScope)
        assert isinstance(DatacenterScope(datacenter="dc"), RoutingScope)
        assert isinstance(RackScope(datacenter="dc", rack="r"), RoutingScope)
