"""Lazy query plan for request-scoped node routing."""

from __future__ import annotations

from collections.abc import Iterator, Sequence

from alternator.core.go_rand import GoRand


class LazyQueryPlan(Iterator[str]):
    """
    Lazy iterator of node addresses for request routing.

    Expects nodes to be pre-sorted. The caller (LiveNodesManagerCore.update_nodes)
    is responsible for sorting nodes at ingestion time to ensure deterministic
    ordering across all clients.

    Truly lazy: each call to ``__next__`` picks a random node from the
    remaining pool via incremental Fisher-Yates (seeded for determinism)
    and swaps it into place.  No upfront shuffle is performed.

    Yields raw node addresses (e.g. ``"192.168.1.1"``).

    Raises StopIteration when all nodes have been yielded.
    """

    def __init__(
        self,
        nodes: Sequence[str],
        seed: int,
    ) -> None:
        self._nodes = list(nodes)
        self._rng = GoRand(seed)
        self._index = 0

    def __next__(self) -> str:
        if self._index >= len(self._nodes):
            raise StopIteration

        # Incremental Fisher-Yates: pick a random node from the remaining
        # pool [_index, len) and swap it to the current position.
        remaining = len(self._nodes) - self._index
        pick = self._index + self._rng.intn(remaining)
        self._nodes[self._index], self._nodes[pick] = (
            self._nodes[pick],
            self._nodes[self._index],
        )

        node = self._nodes[self._index]
        self._index += 1
        return node
