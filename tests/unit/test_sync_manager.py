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

"""Tests for SyncLiveNodesManager."""

import time
from collections.abc import Sequence
from urllib.parse import urlsplit

import pytest

from alternator._http import create_sync_http_fetcher
from alternator.config import Config, KeyRouteAffinityConfig, KeyRouteAffinityMode
from alternator.core.live_nodes import NoNodesAvailableError, SyncLiveNodesManager
from alternator.core.routing_scope import ClusterScope, DatacenterScope, RackScope
from alternator.exceptions import ConfigurationError
from tests.conftest import FakeAlternatorServer


class TestSyncLiveNodesManager:
    """Tests for SyncLiveNodesManager."""

    @pytest.fixture
    def config(self) -> Config:
        """Create test configuration."""
        from alternator.config import NodeListPollingConfig

        return Config(
            seed_hosts=["192.168.1.1"],
            port=8000,
            node_list_polling=NodeListPollingConfig(
                active_interval_ms=100, idle_interval_ms=500
            ),
        )

    def test_start_stop_lifecycle(self, config: Config) -> None:
        """Test manager starts and stops cleanly."""
        nodes_returned: list[str] = ["node1", "node2"]

        def mock_fetch(url: str) -> Sequence[str]:
            return nodes_returned

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.start()

        # Give background thread time to run
        time.sleep(0.15)

        # Should have fetched nodes
        assert len(manager.nodes) > 0

        # Stop should complete without hanging
        manager.stop()

    def test_next_node_uri_format(self, config: Config) -> None:
        """Test next_node_uri returns correct format."""

        def mock_fetch(url: str) -> Sequence[str]:
            return ["192.168.1.1", "192.168.1.2"]

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()

        uri = manager.next_node_uri()

        assert uri.startswith("http://")
        assert ":8000" in uri
        assert "192.168.1" in uri

    def test_next_node_uri_raises_when_empty(self, config: Config) -> None:
        """Test next_node_uri raises when no nodes available."""

        def mock_fetch(url: str) -> Sequence[str]:
            return []

        manager = SyncLiveNodesManager(config, mock_fetch)

        with pytest.raises(NoNodesAvailableError):
            manager.next_node_uri()

    def test_fallback_chain_on_empty(self, config: Config) -> None:
        """Test explicit fallback chain is used when scope returns empty."""
        dc_config = Config(
            seed_hosts=["192.168.1.1"],
            port=8000,
            routing_scope=DatacenterScope("dc1", fallback=ClusterScope()),
        )

        calls: list[str] = []

        def mock_fetch(url: str) -> Sequence[str]:
            calls.append(url)
            # Return empty for DC scope, nodes for cluster scope
            if "dc=dc1" in url:
                return []
            return ["node1", "node2"]

        manager = SyncLiveNodesManager(dc_config, mock_fetch)
        result = manager.refresh_nodes()

        assert result is True
        assert len(calls) == 2  # DC scope then cluster scope
        assert "dc=dc1" in calls[0]
        assert "dc=" not in calls[1]

    def test_cluster_scope_aggregates_all_seed_localnodes(self) -> None:
        """Cluster scope unions local-DC node lists returned by each seed."""
        config = Config(
            seed_hosts=["seed-dc1", "seed-dc2"],
            port=8000,
            routing_scope=ClusterScope(),
        )
        calls: list[str] = []

        def mock_fetch(url: str) -> Sequence[str]:
            calls.append(url)
            if "seed-dc1" in url:
                return ["dc1-node1", "dc1-node2"]
            if "seed-dc2" in url:
                return ["dc2-node1", "dc2-node2"]
            return []

        manager = SyncLiveNodesManager(config, mock_fetch)

        assert manager.refresh_nodes() is True
        assert calls == [
            "http://seed-dc1:8000/localnodes",
            "http://seed-dc2:8000/localnodes",
        ]
        assert manager.nodes.nodes == (
            "dc1-node1",
            "dc1-node2",
            "dc2-node1",
            "dc2-node2",
        )
        assert manager.affinity_nodes == manager.nodes

    @pytest.mark.parametrize("affinity_enabled", [False, True])
    @pytest.mark.parametrize("empty_seed_response", [False, True])
    def test_cluster_scope_aggregates_reachable_seed_localnodes(
        self, affinity_enabled: bool, empty_seed_response: bool
    ) -> None:
        """Cluster scope keeps usable seed results when another seed fails."""
        config = Config(
            seed_hosts=["seed-dc1", "seed-dc2"],
            port=8000,
            routing_scope=ClusterScope(),
            key_affinity=KeyRouteAffinityConfig(
                mode=(
                    KeyRouteAffinityMode.RMW
                    if affinity_enabled
                    else KeyRouteAffinityMode.NONE
                )
            ),
        )

        def mock_fetch(url: str) -> Sequence[str]:
            if "seed-dc1" in url:
                if empty_seed_response:
                    return []
                raise OSError("seed unavailable")
            return ["dc2-node1", "dc2-node2"]

        manager = SyncLiveNodesManager(config, mock_fetch)

        assert manager.refresh_nodes() is True
        assert manager.nodes.nodes == ("dc2-node1", "dc2-node2")
        assert manager.affinity_nodes == manager.nodes

    def test_rack_scope_tracks_dc_nodes_for_key_affinity(self) -> None:
        """The named DC response spans racks without merging another DC."""
        config = Config(
            seed_hosts=["seed-dc1", "seed-dc2"],
            port=8000,
            routing_scope=RackScope("dc1", "rack1"),
            key_affinity=KeyRouteAffinityConfig(mode=KeyRouteAffinityMode.RMW),
        )
        calls: list[str] = []

        def mock_fetch(url: str) -> list[str]:
            calls.append(url)
            if "rack=" in url:
                return ["rack1-node"]
            if "dc=dc1" in url:
                return ["rack1-node", "rack2-node"]
            return ["other-dc-node"]

        manager = SyncLiveNodesManager(config, mock_fetch)
        assert manager.refresh_nodes() is True
        assert manager.nodes.nodes == ("rack1-node",)
        assert manager.affinity_nodes.nodes == ("rack1-node", "rack2-node")
        assert manager.affinity_nodes.scope_name == "Datacenter"
        assert calls == [
            "http://seed-dc1:8000/localnodes?dc=dc1&rack=rack1",
            "http://seed-dc1:8000/localnodes?dc=dc1",
        ]

    @pytest.mark.parametrize("empty_response", [False, True])
    @pytest.mark.parametrize("failed_seeds", [("seed1",), ("seed1", "seed2")])
    def test_failed_affinity_refresh_invalidates_and_recovers(
        self, empty_response: bool, failed_seeds: tuple[str, ...]
    ) -> None:
        """Exhausted DC discovery clears affinity, retaining ordinary routing."""
        failures: tuple[str, ...] = ()
        rack_nodes = ["rack1-node"]
        observed: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
        dc_nodes = ["rack1-node", "rack2-node"]

        def mock_fetch(url: str) -> list[str]:
            if "rack=" in url:
                return rack_nodes
            observed.append((manager.nodes.nodes, manager.affinity_nodes.nodes))
            if any(seed in url for seed in failures):
                if empty_response:
                    return []
                raise OSError("DC unavailable")
            return dc_nodes

        def assert_inflight(expected_affinity: tuple[str, ...]) -> None:
            assert observed
            assert all(
                ordinary == tuple(rack_nodes) and affinity == expected_affinity
                for ordinary, affinity in observed
            )
            observed.clear()

        config = Config(
            seed_hosts=["seed1", "seed2"],
            port=8000,
            routing_scope=RackScope("dc1", "rack1"),
            key_affinity=KeyRouteAffinityConfig(mode=KeyRouteAffinityMode.RMW),
        )
        manager = SyncLiveNodesManager(config, mock_fetch)
        assert manager.refresh_nodes() is True
        assert_inflight(())
        assert manager.affinity_nodes.nodes == ("rack1-node", "rack2-node")
        previous_affinity = manager.affinity_nodes.nodes
        rack_nodes = ["rack1-new"]
        failures = failed_seeds
        assert manager.refresh_nodes() is True
        assert_inflight(previous_affinity)
        assert manager.nodes.nodes == ("rack1-new",)
        assert bool(manager.affinity_nodes) == (len(failed_seeds) == 1)
        previous_affinity = manager.affinity_nodes.nodes
        failures = ()
        assert manager.refresh_nodes() is True
        assert_inflight(previous_affinity)
        assert manager.affinity_nodes.nodes == ("rack1-node", "rack2-node")
        previous_affinity = manager.affinity_nodes.nodes
        dc_nodes = ["rack1-node", "replacement-node"]
        assert manager.refresh_nodes() is True
        assert_inflight(previous_affinity)
        assert manager.nodes.nodes == ("rack1-new",)
        assert manager.affinity_nodes.nodes == ("rack1-node", "replacement-node")

    @pytest.mark.parametrize("rack_available", [False, True])
    def test_derived_affinity_chain_reuses_results_and_recovers(
        self, rack_available: bool
    ) -> None:
        """Duplicate derived DC queries run once per refresh and retry next refresh."""
        calls: list[str] = []
        dc_available = False

        def mock_fetch(url: str) -> list[str]:
            query = url.split("?", 1)[1] if "?" in url else ""
            calls.append(query)
            if "rack=" in query:
                return ["rack-node"] if rack_available else []
            if query == "dc=a":
                return ["dc-node"] if dc_available else []
            return ["cluster-node"]

        config = Config(
            seed_hosts=["seed1", "seed2"],
            port=8000,
            routing_scope=RackScope(
                "a",
                "A1",
                fallback=RackScope(
                    "a",
                    "A1",
                    fallback=DatacenterScope("a", fallback=ClusterScope()),
                ),
            ),
            key_affinity=KeyRouteAffinityConfig(mode=KeyRouteAffinityMode.RMW),
        )
        manager = SyncLiveNodesManager(config, mock_fetch)
        assert manager.refresh_nodes() is True
        assert calls == (
            ["dc=a&rack=A1", "dc=a", "dc=a", "", ""]
            if rack_available
            else ["dc=a&rack=A1"] * 4 + ["dc=a", "dc=a", "", ""]
        )
        assert manager.affinity_nodes.nodes == ("cluster-node",)

        calls.clear()
        dc_available = True
        assert manager.refresh_nodes() is True
        assert calls == (
            ["dc=a&rack=A1", "dc=a"]
            if rack_available
            else ["dc=a&rack=A1"] * 4 + ["dc=a"]
        )
        assert manager.affinity_nodes.nodes == ("dc-node",)
        assert manager.affinity_nodes.scope_name == "Datacenter"

    @pytest.mark.parametrize("selected_dc", ["dc1", "dc2", "cluster"])
    def test_affinity_preserves_full_explicit_fallback_chain(
        self, selected_dc: str
    ) -> None:
        """Try both named DCs in order before the explicit cluster fallback."""
        calls: list[str] = []

        def mock_fetch(url: str) -> list[str]:
            query = url.split("?", 1)[1] if "?" in url else ""
            calls.append(query)
            if "rack=" in query:
                return ["rack-node"]
            if query == f"dc={selected_dc}":
                return [f"{selected_dc}-node"]
            if not query:
                if "seed2" in url:
                    raise OSError("one cluster seed unavailable")
                return ["cluster-node"]
            return []

        config = Config(
            seed_hosts=["seed1", "seed2"],
            port=8000,
            routing_scope=RackScope(
                "dc1",
                "rack1",
                fallback=RackScope("dc2", "rack2", fallback=ClusterScope()),
            ),
            key_affinity=KeyRouteAffinityConfig(mode=KeyRouteAffinityMode.RMW),
        )
        manager = SyncLiveNodesManager(config, mock_fetch)
        assert manager.refresh_nodes() is True
        expected = ["dc=dc1&rack=rack1"] + ["dc=dc1"] * (
            1 if selected_dc == "dc1" else 2
        )
        if selected_dc != "dc1":
            expected.extend(["dc=dc2"] * (1 if selected_dc == "dc2" else 2))
        if selected_dc == "cluster":
            expected.extend(["", ""])
        assert calls == expected
        assert manager.nodes.nodes == ("rack-node",)
        assert manager.affinity_nodes.nodes == (f"{selected_dc}-node",)

    @pytest.mark.parametrize("preferred_available", [False, True])
    def test_affinity_reuses_ordinary_fallback_without_skipping_preferred_dc(
        self, preferred_available: bool
    ) -> None:
        """An ordinary later-DC result cannot bypass an earlier affinity DC."""
        calls: list[str] = []

        def mock_fetch(url: str) -> list[str]:
            query = url.split("?", 1)[1]
            calls.append(query)
            if "rack=" in query:
                return []
            if query == "dc=dc1":
                return ["dc1-node"] if preferred_available else []
            return ["dc2-node"]

        config = Config(
            seed_hosts=["seed"],
            port=8000,
            routing_scope=RackScope("dc1", "rack1", fallback=DatacenterScope("dc2")),
            key_affinity=KeyRouteAffinityConfig(mode=KeyRouteAffinityMode.RMW),
        )
        manager = SyncLiveNodesManager(config, mock_fetch)
        assert manager.refresh_nodes() is True
        assert calls == ["dc=dc1&rack=rack1", "dc=dc2", "dc=dc1"]
        assert manager.nodes.nodes == ("dc2-node",)
        assert manager.affinity_nodes.nodes == (
            "dc1-node" if preferred_available else "dc2-node",
        )

    @pytest.mark.parametrize("preferred_available", [False, True])
    def test_cluster_fallback_reuses_partial_results_without_bypassing_preferred_dc(
        self, preferred_available: bool
    ) -> None:
        """Ordinary cluster fallback cannot replace an earlier usable affinity DC."""
        calls: list[str] = []

        def mock_fetch(url: str) -> list[str]:
            query = url.split("?", 1)[1] if "?" in url else ""
            calls.append(query)
            if "rack=" in query:
                return []
            if not query:
                if "seed2" in url:
                    raise OSError("one cluster seed unavailable")
                return ["other-dc-node"]
            # No broader interim snapshot may escape while DC discovery runs.
            assert not manager.affinity_nodes
            return ["dc1-node"] if preferred_available else []

        config = Config(
            seed_hosts=["seed1", "seed2"],
            port=8000,
            routing_scope=RackScope("dc1", "rack1", fallback=ClusterScope()),
            key_affinity=KeyRouteAffinityConfig(mode=KeyRouteAffinityMode.RMW),
        )
        manager = SyncLiveNodesManager(config, mock_fetch)
        assert manager.refresh_nodes() is True
        assert manager.nodes.nodes == ("other-dc-node",)
        assert manager.affinity_nodes.nodes == (
            ("dc1-node",) if preferred_available else ("other-dc-node",)
        )
        assert calls == ["dc=dc1&rack=rack1"] * 2 + [""] * 2 + ["dc=dc1"] * (
            1 if preferred_available else 2
        )

    @pytest.mark.parametrize("affinity_enabled", [False, True])
    @pytest.mark.parametrize("scope_kind", ["dc", "rack", "custom"])
    def test_dc_scope_does_not_duplicate_discovery(
        self, affinity_enabled: bool, scope_kind: str
    ) -> None:
        """The ordinary DC snapshot already satisfies the affinity scope."""

        class CustomRackScope(RackScope):
            def get_localnodes_query(self) -> str:
                return "custom=preserved"

        scopes = {
            "dc": DatacenterScope("dc1"),
            "rack": RackScope("dc1", "rack1"),
            "custom": CustomRackScope("dc1", "rack1"),
        }
        scope = scopes[scope_kind]
        calls: list[str] = []

        def mock_fetch(url: str) -> list[str]:
            calls.append(url)
            return ["dc-node"]

        config = Config(
            seed_hosts=["seed"],
            port=8000,
            routing_scope=scope,
            key_affinity=KeyRouteAffinityConfig(
                mode=(
                    KeyRouteAffinityMode.RMW
                    if affinity_enabled
                    else KeyRouteAffinityMode.NONE
                )
            ),
        )
        manager = SyncLiveNodesManager(config, mock_fetch)
        assert manager.refresh_nodes() is True
        query = scope.get_localnodes_query()
        expected = [f"http://seed:8000/localnodes?{query}"]
        if scope_kind == "rack" and affinity_enabled:
            expected.append("http://seed:8000/localnodes?dc=dc1")
        assert calls == expected
        if affinity_enabled:
            assert manager.affinity_nodes.nodes == manager.nodes.nodes

    def test_dns_entrypoint_discovers_dns_node_records(
        self,
        fake_alternator_server: FakeAlternatorServer,
    ) -> None:
        """Discovery reaches a resolver-backed seed host and keeps DNS node records."""
        fake_alternator_server.set_localnodes(["127.0.0.1", "127.0.0.2"])
        parsed = urlsplit(fake_alternator_server.url("/"))
        assert parsed.port is not None
        config = Config(
            seed_hosts=["localhost"],
            port=parsed.port,
            routing_scope=ClusterScope(),
        )
        manager = SyncLiveNodesManager(
            config,
            create_sync_http_fetcher(timeout_seconds=1.0),
        )

        assert manager.refresh_nodes() is True

        assert fake_alternator_server.requested_paths() == ["/localnodes"]
        assert manager.nodes.nodes == ("127.0.0.1", "127.0.0.2")

    def test_url_construction(self, config: Config) -> None:
        """Test URL is constructed correctly."""
        captured_url: list[str] = []

        def mock_fetch(url: str) -> Sequence[str]:
            captured_url.append(url)
            return ["node1"]

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()

        assert captured_url[0] == "http://192.168.1.1:8000/localnodes"

    def test_url_construction_brackets_ipv6_seed(self) -> None:
        """Test raw IPv6 seed hosts are bracketed in discovery URLs."""
        config = Config(seed_hosts=["2001:db8::1"], port=8000)
        captured_url: list[str] = []

        def mock_fetch(url: str) -> Sequence[str]:
            captured_url.append(url)
            return ["node1"]

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()

        assert captured_url[0] == "http://[2001:db8::1]:8000/localnodes"

    def test_next_node_uri_brackets_ipv6_node(self) -> None:
        """Test raw IPv6 live nodes are bracketed in operation URLs."""
        config = Config(seed_hosts=["seed"], port=8000)

        def mock_fetch(url: str) -> Sequence[str]:
            return ["2001:db8::2"]

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()

        assert manager.next_node_uri() == "http://[2001:db8::2]:8000"

    def test_refresh_recovers_through_original_ipv6_entrypoint(self) -> None:
        """Later refreshes keep using the original seed after nodes are learned."""
        config = Config(seed_hosts=["2001:db8::1"], port=8000)
        calls: list[str] = []

        def mock_fetch(url: str) -> Sequence[str]:
            calls.append(url)
            if len(calls) == 1:
                return ["2001:db8::2"]
            if len(calls) == 2:
                return []
            return ["2001:db8::3"]

        manager = SyncLiveNodesManager(config, mock_fetch)

        assert manager.refresh_nodes() is True
        assert manager.nodes.nodes == ("2001:db8::2",)
        assert manager.refresh_nodes() is False
        assert manager.nodes.nodes == ("2001:db8::2",)
        assert manager.refresh_nodes() is True
        assert manager.nodes.nodes == ("2001:db8::3",)
        assert calls == [
            "http://[2001:db8::1]:8000/localnodes",
            "http://[2001:db8::1]:8000/localnodes",
            "http://[2001:db8::1]:8000/localnodes",
        ]

    def test_url_construction_with_dc_scope(self) -> None:
        """Test URL includes query string for datacenter scope."""
        config = Config(
            seed_hosts=["host1"],
            port=9000,
            routing_scope=DatacenterScope(datacenter="us-east"),
        )
        captured_url: list[str] = []

        def mock_fetch(url: str) -> Sequence[str]:
            captured_url.append(url)
            return ["node1"]

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()

        assert "?dc=us-east" in captured_url[0]

    def test_background_refresh_updates_nodes(self, config: Config) -> None:
        """Test background thread updates nodes."""
        call_count = 0
        nodes_list = [["initial"], ["updated1", "updated2"]]

        def mock_fetch(url: str) -> Sequence[str]:
            nonlocal call_count
            result = nodes_list[min(call_count, len(nodes_list) - 1)]
            call_count += 1
            return result

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()  # Initial fetch
        manager.start()

        # Wait for background refresh
        time.sleep(0.25)

        manager.stop()

        # Should have been called multiple times
        assert call_count >= 2

    def test_https_url_construction(self) -> None:
        """Test HTTPS URL is constructed correctly."""
        config = Config(
            seed_hosts=["secure-host"],
            port=8443,
            scheme="https",
        )
        captured_url: list[str] = []

        def mock_fetch(url: str) -> Sequence[str]:
            captured_url.append(url)
            return ["node1"]

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()

        assert captured_url[0].startswith("https://")
        assert ":8443" in captured_url[0]

    def test_round_robin_selection(self, config: Config) -> None:
        """Test nodes are selected in round-robin order."""

        def mock_fetch(url: str) -> Sequence[str]:
            return ["node1", "node2", "node3"]

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()

        uris = [manager.next_node_uri() for _ in range(6)]
        hosts = [uri.split("://")[1].split(":")[0] for uri in uris]

        assert hosts == ["node1", "node2", "node3", "node1", "node2", "node3"]

    def test_refresh_failure_keeps_existing_nodes(self, config: Config) -> None:
        """Ordinary exhaustion retains nodes and skips even a healthy affinity DC."""
        rack_available = True
        calls: list[str] = []

        def mock_fetch(url: str) -> list[str]:
            query = url.split("?", 1)[1]
            calls.append(query)
            if "rack=" in query:
                return ["rack-node"] if rack_available else []
            return ["rack-node", "dc-node"]

        scoped_config = Config(
            seed_hosts=["seed"],
            port=config.port,
            routing_scope=RackScope("dc1", "rack1"),
            key_affinity=KeyRouteAffinityConfig(mode=KeyRouteAffinityMode.RMW),
        )
        manager = SyncLiveNodesManager(scoped_config, mock_fetch)
        assert manager.refresh_nodes() is True
        assert manager.affinity_nodes.nodes == ("dc-node", "rack-node")
        initial_nodes = manager.nodes
        calls.clear()
        rack_available = False
        assert manager.refresh_nodes() is False
        assert manager.nodes == initial_nodes
        assert not manager.affinity_nodes
        assert calls == ["dc=dc1&rack=rack1"]

    def test_topology_validation_uses_scope_without_state_update(self) -> None:
        """Validation queries one scoped endpoint without replacing live nodes."""
        config = Config(
            seed_hosts=["seed1"],
            port=8000,
            routing_scope=DatacenterScope("dc1", fallback=None),
        )
        calls: list[str] = []

        def mock_fetch(url: str) -> Sequence[str]:
            calls.append(url)
            if "dc=dc1" in url:
                return ["node1"]
            return []

        manager = SyncLiveNodesManager(config, mock_fetch)

        assert manager.check_rack_and_datacenter_set_correctly() is True
        assert manager.check_rack_datacenter_feature_supported() is True
        assert manager.nodes.nodes == ()
        assert calls == [
            "http://seed1:8000/localnodes?dc=dc1",
            "http://seed1:8000/localnodes?dc=dc1",
        ]

    def test_topology_validation_raises_for_missing_scope(self) -> None:
        """Validation raises a clear error when no scoped nodes exist."""
        config = Config(
            seed_hosts=["seed1"],
            port=8000,
            routing_scope=RackScope("dc1", "missing", fallback=None),
        )

        def mock_fetch(url: str) -> Sequence[str]:
            return []

        manager = SyncLiveNodesManager(config, mock_fetch)

        assert manager.check_rack_datacenter_feature_supported() is False
        with pytest.raises(ConfigurationError, match="No nodes found"):
            manager.check_rack_and_datacenter_set_correctly()

    def test_topology_validation_rejects_empty_scope_values(self) -> None:
        """Validation rejects incomplete rack/datacenter config."""
        config = Config(
            seed_hosts=["seed1"],
            port=8000,
            routing_scope=RackScope("dc1", "", fallback=None),
        )

        def mock_fetch(url: str) -> Sequence[str]:
            return ["node1"]

        manager = SyncLiveNodesManager(config, mock_fetch)

        assert manager.check_rack_datacenter_feature_supported() is False
        with pytest.raises(ConfigurationError, match="non-empty"):
            manager.check_rack_and_datacenter_set_correctly()
