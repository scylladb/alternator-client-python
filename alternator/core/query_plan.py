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
