"""Lazy query plan for request-scoped node routing."""

from __future__ import annotations

from collections.abc import Iterator, Sequence

from alternator.core.deterministic_rand import DeterministicRand


class LazyQueryPlan(Iterator[str]):
    """
    Lazy iterator of node addresses for request routing.

    Expects nodes to be pre-sorted. The caller (LiveNodesManagerCore.update_nodes)
    is responsible for sorting nodes at ingestion time to ensure deterministic
    ordering across all clients.

    Truly lazy: each call to ``__next__`` picks a random node from the
    remaining pool, removes it by replacing it with the last remaining node,
    and then truncates the pool. No upfront shuffle is performed.

    Yields raw node addresses (e.g. ``"192.168.1.1"``).

    Raises StopIteration when all nodes have been yielded.
    """

    def __init__(
        self,
        nodes: Sequence[str],
        seed: int,
    ) -> None:
        self._nodes = list(nodes)
        self._rng = DeterministicRand(seed)

    def __next__(self) -> str:
        if not self._nodes:
            raise StopIteration

        # Cross-client-compatible pick-and-remove: pick from the current pool,
        # return that node, then fill its slot with the last remaining node.
        pick = self._rng.intn(len(self._nodes))
        node = self._nodes[pick]
        last = self._nodes.pop()
        if pick < len(self._nodes):
            self._nodes[pick] = last

        return node
