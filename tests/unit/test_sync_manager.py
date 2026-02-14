"""Tests for SyncLiveNodesManager."""

import time
from collections.abc import Sequence

import pytest

from alternator.config import AlternatorConfig
from alternator.core.live_nodes import NoNodesAvailableError, SyncLiveNodesManager
from alternator.core.routing_scope import DatacenterScope


class TestSyncLiveNodesManager:
    """Tests for SyncLiveNodesManager."""

    @pytest.fixture
    def config(self) -> AlternatorConfig:
        """Create test configuration."""
        return AlternatorConfig(
            seed_hosts=["192.168.1.1"],
            port=8000,
            active_refresh_interval_ms=100,
            idle_refresh_interval_ms=500,
        )

    def test_start_stop_lifecycle(self, config: AlternatorConfig) -> None:
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

    def test_next_node_uri_format(self, config: AlternatorConfig) -> None:
        """Test next_node_uri returns correct format."""

        def mock_fetch(url: str) -> Sequence[str]:
            return ["192.168.1.1", "192.168.1.2"]

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()

        uri = manager.next_node_uri()

        assert uri.startswith("http://")
        assert ":8000" in uri
        assert "192.168.1" in uri

    def test_next_node_uri_raises_when_empty(self, config: AlternatorConfig) -> None:
        """Test next_node_uri raises when no nodes available."""

        def mock_fetch(url: str) -> Sequence[str]:
            return []

        manager = SyncLiveNodesManager(config, mock_fetch)

        with pytest.raises(NoNodesAvailableError):
            manager.next_node_uri()

    def test_fallback_chain_on_empty(self, config: AlternatorConfig) -> None:
        """Test fallback chain is used when scope returns empty."""
        dc_config = AlternatorConfig(
            seed_hosts=["192.168.1.1"],
            port=8000,
            routing_scope=DatacenterScope(datacenter="dc1"),
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

    def test_url_construction(self, config: AlternatorConfig) -> None:
        """Test URL is constructed correctly."""
        captured_url: list[str] = []

        def mock_fetch(url: str) -> Sequence[str]:
            captured_url.append(url)
            return ["node1"]

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()

        assert captured_url[0] == "http://192.168.1.1:8000/localnodes"

    def test_url_construction_with_dc_scope(self) -> None:
        """Test URL includes query string for datacenter scope."""
        config = AlternatorConfig(
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

    def test_background_refresh_updates_nodes(self, config: AlternatorConfig) -> None:
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
        config = AlternatorConfig(
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

    def test_round_robin_selection(self, config: AlternatorConfig) -> None:
        """Test nodes are selected in round-robin order."""

        def mock_fetch(url: str) -> Sequence[str]:
            return ["node1", "node2", "node3"]

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()

        uris = [manager.next_node_uri() for _ in range(6)]
        hosts = [uri.split("://")[1].split(":")[0] for uri in uris]

        assert hosts == ["node1", "node2", "node3", "node1", "node2", "node3"]

    def test_refresh_failure_keeps_existing_nodes(
        self, config: AlternatorConfig
    ) -> None:
        """Test failed refresh keeps existing nodes."""
        call_count = 0

        def mock_fetch(url: str) -> Sequence[str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return ["node1", "node2"]
            return []  # Subsequent calls fail

        manager = SyncLiveNodesManager(config, mock_fetch)
        manager.refresh_nodes()  # Succeeds

        initial_nodes = manager.nodes.nodes
        manager.refresh_nodes()  # Fails, but keeps nodes

        # Nodes should still be available
        assert manager.nodes.nodes == initial_nodes
