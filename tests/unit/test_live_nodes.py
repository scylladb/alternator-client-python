"""Tests for live nodes management and selection."""

import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pytest

from alternator.config import AlternatorConfig, NodeListPollingConfig
from alternator.core.live_nodes import (
    LiveNodesManagerCore,
    NodeList,
    RoundRobinSelector,
)
from alternator.core.routing_scope import ClusterScope


class TestNodeList:
    """Tests for NodeList dataclass."""

    def test_empty_nodelist(self) -> None:
        """Test empty node list."""
        nodes = NodeList(nodes=(), scope_name="Cluster")
        assert len(nodes) == 0
        assert bool(nodes) is False

    def test_nodelist_with_nodes(self) -> None:
        """Test node list with nodes."""
        nodes = NodeList(
            nodes=("192.168.1.1", "192.168.1.2", "192.168.1.3"),
            scope_name="Cluster",
        )
        assert len(nodes) == 3
        assert bool(nodes) is True

    def test_nodelist_is_immutable(self) -> None:
        """Test that NodeList is frozen."""
        nodes = NodeList(nodes=("host1",), scope_name="Cluster")
        with pytest.raises(AttributeError):
            nodes.nodes = ("host2",)  # type: ignore[misc] -- testing frozen dataclass immutability


class TestRoundRobinSelector:
    """Tests for RoundRobinSelector."""

    def test_select_from_empty_returns_none(self) -> None:
        """Test selecting from empty node list returns None."""
        selector = RoundRobinSelector()
        nodes = NodeList(nodes=(), scope_name="Cluster")
        assert selector.select(nodes) is None

    def test_select_distributes_evenly(self) -> None:
        """Test round-robin distributes requests evenly."""
        selector = RoundRobinSelector()
        nodes = NodeList(
            nodes=("host1", "host2", "host3"),
            scope_name="Cluster",
        )

        # Select 9 times (3 full rounds)
        selections = [selector.select(nodes) for _ in range(9)]

        # Each host should be selected 3 times
        counter: Counter[str | None] = Counter(selections)
        assert counter["host1"] == 3
        assert counter["host2"] == 3
        assert counter["host3"] == 3

    def test_select_order_is_deterministic(self) -> None:
        """Test round-robin order is deterministic."""
        selector = RoundRobinSelector()
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="Cluster")

        # First round should go a -> b -> c
        assert selector.select(nodes) == "a"
        assert selector.select(nodes) == "b"
        assert selector.select(nodes) == "c"
        # Second round repeats
        assert selector.select(nodes) == "a"

    def test_select_thread_safety(self) -> None:
        """Test round-robin selector is thread-safe."""
        selector = RoundRobinSelector()
        nodes = NodeList(
            nodes=("host1", "host2", "host3"),
            scope_name="Cluster",
        )

        results: list[str | None] = []
        lock = threading.Lock()

        def select_many() -> None:
            for _ in range(100):
                result = selector.select(nodes)
                with lock:
                    results.append(result)

        # Run 10 threads, each selecting 100 times
        threads = [threading.Thread(target=select_many) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Total selections should be 1000
        assert len(results) == 1000

        # Distribution should be roughly even (allowing for timing variations)
        counter: Counter[str | None] = Counter(results)
        assert counter["host1"] > 300
        assert counter["host2"] > 300
        assert counter["host3"] > 300


class TestLiveNodesManagerCore:
    """Tests for LiveNodesManagerCore."""

    @pytest.fixture
    def config(self) -> AlternatorConfig:
        """Create test configuration."""
        return AlternatorConfig(
            seed_hosts=["localhost"],
            port=8000,
            node_list_polling=NodeListPollingConfig(
                active_interval_ms=100, idle_interval_ms=1000
            ),
        )

    def test_initial_state_empty(self, config: AlternatorConfig) -> None:
        """Test initial state has empty node list."""
        manager = LiveNodesManagerCore(config)
        assert len(manager.nodes) == 0
        assert manager.nodes.scope_name == ""

    def test_update_nodes(self, config: AlternatorConfig) -> None:
        """Test updating node list."""
        manager = LiveNodesManagerCore(config)
        scope = ClusterScope()

        manager.update_nodes(["host1", "host2"], scope)

        assert len(manager.nodes) == 2
        assert manager.nodes.scope_name == "Cluster"
        assert "host1" in manager.nodes.nodes
        assert "host2" in manager.nodes.nodes

    def test_next_node_round_robin(self, config: AlternatorConfig) -> None:
        """Test next_node uses round-robin selection."""
        manager = LiveNodesManagerCore(config)
        manager.update_nodes(["a", "b", "c"], ClusterScope())

        selections = [manager.next_node() for _ in range(6)]
        assert selections == ["a", "b", "c", "a", "b", "c"]

    def test_next_node_returns_none_when_empty(self, config: AlternatorConfig) -> None:
        """Test next_node returns None when no nodes available."""
        manager = LiveNodesManagerCore(config)
        assert manager.next_node() is None

    def test_active_refresh_interval(self, config: AlternatorConfig) -> None:
        """Test active refresh interval when recently active."""
        manager = LiveNodesManagerCore(config)
        manager.update_nodes(["host1"], ClusterScope())

        # Trigger activity
        manager.next_node()

        # Should return active interval
        interval = manager.get_refresh_interval_seconds()
        assert interval == 0.1  # 100ms

    def test_idle_refresh_interval(self, config: AlternatorConfig) -> None:
        """Test idle refresh interval after inactivity."""
        config_short = AlternatorConfig(
            seed_hosts=["localhost"],
            port=8000,
            node_list_polling=NodeListPollingConfig(
                active_interval_ms=100,
                idle_interval_ms=200,  # Short for testing
            ),
        )
        manager = LiveNodesManagerCore(config_short)

        # Wait well past the idle threshold to avoid flaky results on slow CI
        time.sleep(0.5)

        interval = manager.get_refresh_interval_seconds()
        assert interval == 0.2  # 200ms (idle)

    def test_nodes_property_thread_safe(self, config: AlternatorConfig) -> None:
        """Test nodes property is thread-safe."""
        manager = LiveNodesManagerCore(config)
        results: list[NodeList] = []
        errors: list[Exception] = []

        def update_nodes() -> None:
            try:
                for i in range(100):
                    manager.update_nodes([f"host{i}"], ClusterScope())
            except Exception as e:
                errors.append(e)

        def read_nodes() -> None:
            try:
                for _ in range(100):
                    nodes = manager.nodes
                    results.append(nodes)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=update_nodes),
            threading.Thread(target=read_nodes),
            threading.Thread(target=read_nodes),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 200


class TestRoundRobinConcurrency:
    """Additional concurrency tests for round-robin selection."""

    def test_high_concurrency_distribution(self) -> None:
        """Test distribution under high concurrency."""
        selector = RoundRobinSelector()
        nodes = NodeList(nodes=("a", "b", "c", "d", "e"), scope_name="test")

        results: list[str | None] = []

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(selector.select, nodes) for _ in range(10000)]
            results = [f.result() for f in futures]

        counter: Counter[str | None] = Counter(results)

        # With 10000 selections across 5 nodes, each should have ~2000
        # Allow 10% variance
        for node in ("a", "b", "c", "d", "e"):
            assert 1800 < counter[node] < 2200

    def test_concurrent_update_and_select(self) -> None:
        """Test concurrent node updates and selections."""
        config = AlternatorConfig(
            seed_hosts=["localhost"],
            port=8000,
        )
        manager = LiveNodesManagerCore(config)
        manager.update_nodes(["initial"], ClusterScope())

        errors: list[Exception] = []
        selections: list[str | None] = []
        lock = threading.Lock()

        def updater() -> None:
            try:
                for i in range(100):
                    nodes = [f"host{j}" for j in range((i % 5) + 1)]
                    manager.update_nodes(nodes, ClusterScope())
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        def selector() -> None:
            try:
                for _ in range(200):
                    node = manager.next_node()
                    with lock:
                        selections.append(node)
            except Exception as e:
                errors.append(e)

        # Run updaters and selectors concurrently
        threads = [
            threading.Thread(target=updater),
            threading.Thread(target=updater),
            threading.Thread(target=selector),
            threading.Thread(target=selector),
            threading.Thread(target=selector),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(selections) == 600
        # All selections should be valid (not None after initial setup)
        assert all(s is not None for s in selections)
