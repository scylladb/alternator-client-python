"""Tests for ExecutionPlan class."""

from collections import Counter
from unittest import mock

import pytest

from alternator.core.execution_plan import ExecutionPlan


class TestExecutionPlanBasic:
    """Basic tests for ExecutionPlan."""

    def test_next_returns_full_uri(self) -> None:
        """Test that next() returns a full URI."""
        plan = ExecutionPlan(
            nodes=["192.168.1.1", "192.168.1.2"],
            scheme="http",
            port=8000,
        )
        uri = next(plan)
        assert uri.startswith("http://")
        assert ":8000" in uri

    def test_next_https_scheme(self) -> None:
        """Test HTTPS scheme in URI."""
        plan = ExecutionPlan(
            nodes=["host1"],
            scheme="https",
            port=8443,
        )
        uri = next(plan)
        assert uri == "https://host1:8443"

    def test_exhaustion_raises_stop_iteration(self) -> None:
        """Test that StopIteration is raised when exhausted."""
        plan = ExecutionPlan(
            nodes=["a", "b"],
            scheme="http",
            port=8000,
        )

        next(plan)
        next(plan)

        with pytest.raises(StopIteration):
            next(plan)

    def test_empty_nodes_raises_on_first_call(self) -> None:
        """Test that empty node list raises on first call."""
        plan = ExecutionPlan(
            nodes=[],
            scheme="http",
            port=8000,
        )

        with pytest.raises(StopIteration):
            next(plan)

    def test_iterable_protocol(self) -> None:
        """Test that ExecutionPlan works as an iterator."""
        plan = ExecutionPlan(
            nodes=["a", "b", "c"],
            scheme="http",
            port=8000,
        )
        uris = list(plan)
        assert len(uris) == 3
        for uri in uris:
            assert uri.startswith("http://")
            assert ":8000" in uri


class TestExecutionPlanShuffle:
    """Tests for node shuffling behavior."""

    def test_nodes_are_shuffled(self) -> None:
        """Test that nodes are shuffled for load distribution."""
        orders_seen: set[tuple[str, ...]] = set()

        for _ in range(50):
            plan = ExecutionPlan(
                nodes=["a", "b", "c", "d", "e"],
                scheme="http",
                port=8000,
            )
            nodes = []
            for uri in plan:
                node = uri.split("://")[1].split(":")[0]
                nodes.append(node)
            orders_seen.add(tuple(nodes))

        # Should see multiple different orderings
        assert len(orders_seen) > 1

    def test_lazy_shuffle_uses_randrange(self) -> None:
        """Test that lazy shuffle uses randrange for incremental Fisher-Yates."""
        with mock.patch(
            "alternator.core.execution_plan.random.randrange", return_value=0
        ) as mock_randrange:
            plan = ExecutionPlan(
                nodes=["a", "b", "c"],
                scheme="http",
                port=8000,
            )
            # randrange not called until next()
            mock_randrange.assert_not_called()

            next(plan)
            mock_randrange.assert_called_once_with(3)

            next(plan)
            assert mock_randrange.call_count == 2


class TestExecutionPlanAffinityHash:
    """Tests for affinity hash (deterministic ordering) behavior."""

    def test_affinity_hash_produces_deterministic_order(self) -> None:
        """Test that same hash produces same node order."""
        hash_value = 12345

        orders = []
        for _ in range(5):
            plan = ExecutionPlan(
                nodes=["a", "b", "c", "d"],
                scheme="http",
                port=8000,
                affinity_hash=hash_value,
            )
            nodes = [uri.split("://")[1].split(":")[0] for uri in plan]
            orders.append(tuple(nodes))

        # All orders should be identical
        assert all(order == orders[0] for order in orders)

    def test_different_hashes_may_produce_different_orders(self) -> None:
        """Test that different hashes can produce different node orders."""
        orders_seen: set[tuple[str, ...]] = set()

        for hash_value in range(100):
            plan = ExecutionPlan(
                nodes=["a", "b", "c", "d", "e"],
                scheme="http",
                port=8000,
                affinity_hash=hash_value,
            )
            nodes = [uri.split("://")[1].split(":")[0] for uri in plan]
            orders_seen.add(tuple(nodes))

        # Should see multiple different orderings
        assert len(orders_seen) > 1

    def test_affinity_hash_primary_node_selection(self) -> None:
        """Test that primary node is selected based on hash % len."""
        # Sorted nodes: ["a", "b", "c"]
        # hash=0 -> index 0 -> "a"
        # hash=1 -> index 1 -> "b"
        # hash=2 -> index 2 -> "c"
        # hash=3 -> index 0 -> "a"

        for hash_value, expected_primary in [(0, "a"), (1, "b"), (2, "c"), (3, "a")]:
            plan = ExecutionPlan(
                nodes=["c", "a", "b"],  # Unsorted order
                scheme="http",
                port=8000,
                affinity_hash=hash_value,
            )
            uri = next(plan)
            node = uri.split("://")[1].split(":")[0]
            assert node == expected_primary, f"hash={hash_value}"

    def test_affinity_hash_none_uses_lazy_shuffle(self) -> None:
        """Test that None affinity_hash uses lazy incremental shuffling."""
        with mock.patch(
            "alternator.core.execution_plan.random.randrange", return_value=0
        ) as mock_randrange:
            plan = ExecutionPlan(
                nodes=["a", "b", "c"],
                scheme="http",
                port=8000,
                affinity_hash=None,
            )
            # When affinity_hash is None, randrange is used lazily
            mock_randrange.assert_not_called()

            next(plan)
            mock_randrange.assert_called_once()

    def test_affinity_hash_does_not_use_random_shuffle(self) -> None:
        """Test that provided affinity_hash does not use random.shuffle."""
        with mock.patch(
            "alternator.core.execution_plan.random.shuffle"
        ) as mock_shuffle:
            ExecutionPlan(
                nodes=["a", "b", "c"],
                scheme="http",
                port=8000,
                affinity_hash=12345,
            )
            # When affinity_hash is provided, random.shuffle should NOT be called
            mock_shuffle.assert_not_called()

    def test_affinity_hash_retries_use_deterministic_order(self) -> None:
        """Test that retries with same hash follow same order across clients."""
        hash_value = 99999

        plan1 = ExecutionPlan(
            nodes=["node1", "node2", "node3", "node4"],
            scheme="http",
            port=8000,
            affinity_hash=hash_value,
        )
        plan2 = ExecutionPlan(
            nodes=["node1", "node2", "node3", "node4"],
            scheme="http",
            port=8000,
            affinity_hash=hash_value,
        )

        # Both should try nodes in same order
        for uri1, uri2 in zip(plan1, plan2):
            assert uri1 == uri2

    def test_affinity_hash_covers_all_nodes(self) -> None:
        """Test that affinity hash ordering includes all nodes."""
        plan = ExecutionPlan(
            nodes=["a", "b", "c", "d"],
            scheme="http",
            port=8000,
            affinity_hash=42,
        )

        nodes_seen = {uri.split("://")[1].split(":")[0] for uri in plan}
        assert nodes_seen == {"a", "b", "c", "d"}

    def test_negative_affinity_hash_works(self) -> None:
        """Test that negative hash values work correctly."""
        plan = ExecutionPlan(
            nodes=["a", "b", "c"],
            scheme="http",
            port=8000,
            affinity_hash=-12345,
        )

        uri = next(plan)
        assert uri.startswith("http://")
        node = uri.split("://")[1].split(":")[0]
        assert node in ["a", "b", "c"]


class TestExecutionPlanDistribution:
    """Tests for load distribution across execution plans."""

    def test_distribution_across_plans(self) -> None:
        """Test that multiple plans distribute across nodes."""
        first_nodes: Counter[str] = Counter()

        for _ in range(100):
            plan = ExecutionPlan(
                nodes=["a", "b", "c", "d"],
                scheme="http",
                port=8000,
            )
            uri = next(plan)
            node = uri.split("://")[1].split(":")[0]
            first_nodes[node] += 1

        # Each node should be selected a reasonable number of times
        # With 100 runs and 4 nodes, expect ~25 each
        for node in ["a", "b", "c", "d"]:
            assert first_nodes[node] > 10  # At least 10% distribution

    def test_each_plan_tries_all_nodes_before_exhaustion(self) -> None:
        """Test that a single plan tries all nodes."""
        with mock.patch(
            "alternator.core.execution_plan.random.randrange", return_value=0
        ):
            plan = ExecutionPlan(
                nodes=["a", "b", "c"],
                scheme="http",
                port=8000,
            )

            nodes_tried = set()
            for uri in plan:
                node = uri.split("://")[1].split(":")[0]
                nodes_tried.add(node)

        assert nodes_tried == {"a", "b", "c"}
