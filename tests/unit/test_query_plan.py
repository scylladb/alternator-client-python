"""Tests for LazyQueryPlan class."""

from collections import Counter

import pytest

from alternator.core.query_plan import LazyQueryPlan


class TestLazyQueryPlanBasic:
    """Basic tests for LazyQueryPlan."""

    def test_next_returns_node_address(self) -> None:
        """Test that next() returns a raw node address."""
        plan = LazyQueryPlan(
            nodes=["192.168.1.1", "192.168.1.2"],
            seed=42,
        )
        node = next(plan)
        assert node in ["192.168.1.1", "192.168.1.2"]

    def test_exhaustion_raises_stop_iteration(self) -> None:
        """Test that StopIteration is raised when exhausted."""
        plan = LazyQueryPlan(nodes=["a", "b"], seed=0)

        next(plan)
        next(plan)

        with pytest.raises(StopIteration):
            next(plan)

    def test_empty_nodes_raises_on_first_call(self) -> None:
        """Test that empty node list raises on first call."""
        plan = LazyQueryPlan(nodes=[], seed=0)

        with pytest.raises(StopIteration):
            next(plan)

    def test_iterable_protocol(self) -> None:
        """Test that LazyQueryPlan works as an iterator."""
        plan = LazyQueryPlan(nodes=["a", "b", "c"], seed=42)
        nodes = list(plan)
        assert len(nodes) == 3
        assert set(nodes) == {"a", "b", "c"}


class TestLazyQueryPlanSeed:
    """Tests for seed-based deterministic ordering behavior."""

    def test_same_seed_produces_deterministic_order(self) -> None:
        """Test that same seed produces same node order."""
        seed = 12345

        orders = []
        for _ in range(5):
            plan = LazyQueryPlan(nodes=["a", "b", "c", "d"], seed=seed)
            orders.append(tuple(plan))

        # All orders should be identical
        assert all(order == orders[0] for order in orders)

    def test_different_seeds_may_produce_different_orders(self) -> None:
        """Test that different seeds can produce different node orders."""
        orders_seen: set[tuple[str, ...]] = set()

        for seed in range(100):
            plan = LazyQueryPlan(nodes=["a", "b", "c", "d", "e"], seed=seed)
            orders_seen.add(tuple(plan))

        # Should see multiple different orderings
        assert len(orders_seen) > 1

    def test_same_seed_same_first_node(self) -> None:
        """Test that the same seed always picks the same first node."""
        for seed in [0, 1, 2, 42, 99999]:
            firsts = set()
            for _ in range(5):
                plan = LazyQueryPlan(nodes=["a", "b", "c"], seed=seed)
                firsts.add(next(plan))
            assert len(firsts) == 1, f"seed={seed} produced different first nodes"

    def test_retries_use_deterministic_order(self) -> None:
        """Test that retries with same seed follow same order across clients."""
        seed = 99999
        nodes = ["node1", "node2", "node3", "node4"]

        plan1 = LazyQueryPlan(nodes=nodes, seed=seed)
        plan2 = LazyQueryPlan(nodes=nodes, seed=seed)

        # Both should try nodes in same order
        for n1, n2 in zip(plan1, plan2, strict=True):
            assert n1 == n2

    def test_covers_all_nodes(self) -> None:
        """Test that seed-based ordering includes all nodes."""
        plan = LazyQueryPlan(nodes=["a", "b", "c", "d"], seed=42)
        assert set(plan) == {"a", "b", "c", "d"}

    def test_negative_seed_works(self) -> None:
        """Test that negative seed values work correctly."""
        plan = LazyQueryPlan(nodes=["a", "b", "c"], seed=-12345)
        node = next(plan)
        assert node in ["a", "b", "c"]


class TestLazyQueryPlanDistribution:
    """Tests for load distribution across query plans."""

    def test_distribution_across_plans(self) -> None:
        """Test that different seeds distribute across nodes."""
        first_nodes: Counter[str] = Counter()

        for seed in range(100):
            plan = LazyQueryPlan(nodes=["a", "b", "c", "d"], seed=seed)
            first_nodes[next(plan)] += 1

        # Each node should be selected a reasonable number of times
        # With 100 runs and 4 nodes, expect ~25 each
        for node in ["a", "b", "c", "d"]:
            assert first_nodes[node] > 10  # At least 10% distribution

    def test_each_plan_tries_all_nodes_before_exhaustion(self) -> None:
        """Test that a single plan tries all nodes."""
        plan = LazyQueryPlan(nodes=["a", "b", "c"], seed=42)
        assert set(plan) == {"a", "b", "c"}


# Sorted node list matching the spec: node1..node10 sorted lexicographically
_SPEC_NODES = [f"node{i}.example.com:8043" for i in range(1, 11)]
_SPEC_NODES.sort()

MAX_INT64 = (1 << 63) - 1


def _node_short(full: str) -> str:
    """Extract short name like 'node6' from 'node6.example.com:8043'."""
    return full.split(".")[0]


class TestDeterministicVectors:
    """Stable test vectors from the request-routing specification."""

    @pytest.mark.parametrize(
        "seed, expected_first_6",
        [
            (42, ["node5", "node9", "node6", "node4", "node1", "node3"]),
            (123, ["node5", "node10", "node1", "node2", "node9", "node4"]),
            (999, ["node4", "node5", "node10", "node3", "node2", "node7"]),
            (0, ["node4", "node10", "node3", "node7", "node9", "node6"]),
            (-1, ["node10", "node5", "node2", "node1", "node9", "node6"]),
            (12345, ["node3", "node5", "node2", "node9", "node1", "node10"]),
            (MAX_INT64, ["node10", "node7", "node9", "node3", "node5", "node8"]),
        ],
        ids=["seed42", "seed123", "seed999", "seed0", "seed-1", "seed12345", "seedMAX"],
    )
    def test_spec_vector(self, seed: int, expected_first_6: list[str]) -> None:
        """Verify shuffle matches the deterministic spec vectors."""
        plan = LazyQueryPlan(nodes=_SPEC_NODES, seed=seed)
        actual = [_node_short(next(plan)) for _ in range(6)]
        assert actual == expected_first_6, (
            f"seed={seed}: expected {expected_first_6}, got {actual}"
        )
