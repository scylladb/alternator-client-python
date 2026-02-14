"""Lazy execution plan for request-scoped node routing."""

from __future__ import annotations

import random
from collections.abc import Callable, Iterator, Sequence
from typing import TYPE_CHECKING, Protocol

from alternator.exceptions import NoNodesAvailableError

if TYPE_CHECKING:
    from alternator.config import AlternatorConfig
    from alternator.core.live_nodes import NodeList


class _HasNodes(Protocol):
    """Protocol for objects that provide a nodes property."""

    @property
    def nodes(self) -> NodeList: ...


class ExecutionPlan(Iterator[str]):
    """
    Iterator of node URIs for request routing.

    Node ordering behavior:
    - Without affinity (affinity_hash=None): Nodes are randomly shuffled for
      load distribution across clients.
    - With affinity (affinity_hash provided): Nodes are ordered deterministically
      based on the partition key hash. The primary node (hash % len) goes first,
      and remaining nodes are shuffled using the hash as a seed. This ensures
      all clients with the same partition key try nodes in the same order.

    Raises StopIteration when all nodes have been yielded.
    """

    def __init__(
        self,
        nodes: Sequence[str],
        scheme: str,
        port: int,
        affinity_hash: int | None = None,
    ) -> None:
        self._lazy_shuffle = affinity_hash is None
        self._nodes = self._prepare_nodes(list(nodes), affinity_hash)
        self._scheme = scheme
        self._port = port
        self._index = 0

    def _prepare_nodes(self, nodes: list[str], affinity_hash: int | None) -> list[str]:
        if not nodes:
            return []

        if affinity_hash is None:
            # No affinity: keep original order and lazily shuffle
            # via incremental Fisher-Yates in __next__()
            return nodes

        # Affinity: deterministic order based on hash
        # Sort nodes first for consistency across clients
        sorted_nodes = sorted(nodes)
        n = len(sorted_nodes)

        # Primary node uses same logic as AffinitySelector
        primary_index = abs(affinity_hash) % n
        primary = sorted_nodes[primary_index]

        # Remaining nodes in deterministic order using hash as seed
        remaining = sorted_nodes[:primary_index] + sorted_nodes[primary_index + 1 :]

        # Shuffle remaining with hash as seed for deterministic but distributed order
        rng = random.Random(affinity_hash)
        rng.shuffle(remaining)

        return [primary] + remaining

    def __next__(self) -> str:
        if self._index >= len(self._nodes):
            raise StopIteration

        if self._lazy_shuffle:
            # Incremental Fisher-Yates: pick a random node from the remaining
            # pool [_index, len) and swap it to the current position.
            remaining = len(self._nodes) - self._index
            pick = self._index + random.randrange(remaining)
            self._nodes[self._index], self._nodes[pick] = (
                self._nodes[pick],
                self._nodes[self._index],
            )

        node = self._nodes[self._index]
        self._index += 1
        return f"{self._scheme}://{node}:{self._port}"


def create_execution_plan_factory(
    config: AlternatorConfig,
    manager: _HasNodes,
) -> Callable[[int | None], ExecutionPlan]:
    """
    Create factory that produces ExecutionPlans for requests.

    Shared between sync and async clients.

    Args:
        config: Alternator configuration
        manager: Any object with a ``nodes`` property returning a ``NodeList``

    Returns:
        Factory function that creates ExecutionPlan instances
    """

    def create_plan(affinity_hash: int | None = None) -> ExecutionPlan:
        nodes = manager.nodes
        if not nodes:
            raise NoNodesAvailableError(
                "No nodes available",
                scope_name=config.routing_scope.name,
            )
        return ExecutionPlan(
            nodes=nodes.nodes,
            scheme=config.scheme,
            port=config.port,
            affinity_hash=affinity_hash,
        )

    return create_plan
