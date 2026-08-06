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

"""Live nodes management and selection for Alternator load balancing."""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import logging
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from alternator._http import AsyncHttpFetcher, SyncHttpFetcher
from alternator.exceptions import ConfigurationError, NoNodesAvailableError

if TYPE_CHECKING:
    from alternator.config import Config
    from alternator.core.routing_scope import RoutingScope

logger = logging.getLogger("alternator")

# Re-export for backwards compatibility
__all__ = [
    "NoNodesAvailableError",
    "NodeList",
    "SyncLiveNodesManager",
    "AsyncLiveNodesManager",
]


@dataclass(frozen=True)
class NodeList:
    """Immutable snapshot of available nodes."""

    nodes: tuple[str, ...]
    scope_name: str

    def __len__(self) -> int:
        return len(self.nodes)

    def __bool__(self) -> bool:
        return bool(self.nodes)


class RoundRobinSelector:
    """
    Thread-safe round-robin node selector.

    Pure computation - no I/O, works for both sync and async.
    """

    def __init__(self) -> None:
        self._counter = 0
        self._lock = threading.Lock()

    # Wrap counter at 2^63 to prevent unbounded growth while staying within
    # efficient integer range. The exact value doesn't affect round-robin
    # behavior since we always use modulo len(nodes).
    _WRAP_LIMIT = 2**63

    def select(self, nodes: NodeList) -> str | None:
        """Select next node using round-robin."""
        if not nodes:
            return None

        with self._lock:
            index = self._counter % len(nodes)
            self._counter = (self._counter + 1) % self._WRAP_LIMIT

        return nodes.nodes[index]


class LiveNodesManagerCore:
    """
    Core state management for live nodes - no I/O.

    This class manages:
    - Current node list (atomic reference)
    - Round-robin selection
    - Activity tracking for adaptive refresh
    - URL building for /localnodes endpoint
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._nodes: NodeList = NodeList(nodes=(), scope_name="")
        self._nodes_lock = threading.Lock()
        self._selector = RoundRobinSelector()
        self._last_activity = time.monotonic()

    @property
    def nodes(self) -> NodeList:
        """Get current node list (thread-safe)."""
        with self._nodes_lock:
            return self._nodes

    def update_nodes(self, nodes: Sequence[str], scope: RoutingScope) -> None:
        """Update node list (thread-safe).

        Nodes are sorted before storing to ensure deterministic ordering
        across all clients, which is required by LazyQueryPlan for
        consistent affinity-based routing.
        """
        new_list = NodeList(
            nodes=tuple(sorted(nodes)),
            scope_name=scope.name,
        )
        with self._nodes_lock:
            self._nodes = new_list

    def next_node(self) -> str | None:
        """Get next node using round-robin (thread-safe)."""
        with self._nodes_lock:
            self._last_activity = time.monotonic()
        return self._selector.select(self.nodes)

    def get_refresh_interval_seconds(self) -> float:
        """Get appropriate refresh interval based on activity."""
        polling = self._config.node_list_polling
        idle_threshold = polling.idle_interval_ms / 1000.0
        with self._nodes_lock:
            elapsed = time.monotonic() - self._last_activity

        if elapsed < idle_threshold:
            return polling.active_interval_ms / 1000.0
        return polling.idle_interval_ms / 1000.0

    def build_localnodes_url(self, scope: RoutingScope, seed: str) -> str:
        """
        Build the /localnodes URL for a given scope and seed host.

        Args:
            scope: Routing scope to build URL for
            seed: Seed host address to query

        Returns:
            Full URL with query string
        """
        authority = _format_host_port(seed, self._config.port)
        base = f"{self._config.scheme}://{authority}/localnodes"
        query = scope.get_localnodes_query()
        return f"{base}?{query}" if query else base

    def process_fetch_result(
        self,
        nodes: Sequence[str],
        scope: RoutingScope,
    ) -> bool:
        """
        Process a successful fetch result.

        Args:
            nodes: List of nodes returned from fetch
            scope: The scope that was fetched

        Returns:
            True if nodes were updated, False if empty
        """
        if not nodes:
            logger.warning(
                "No nodes returned for scope %s, trying fallback",
                scope.name,
                extra={
                    "event": "scope_fallback",
                    "scope": scope.name,
                    "reason": "empty",
                },
            )
            return False

        logger.info(
            "Discovered %d nodes for scope %s",
            len(nodes),
            scope.name,
            extra={
                "event": "node_discovery",
                "scope": scope.name,
                "node_count": len(nodes),
            },
        )
        logger.debug(
            "Node list: %s",
            nodes,
            extra={"event": "node_list", "scope": scope.name, "nodes": list(nodes)},
        )
        self.update_nodes(nodes, scope)
        return True

    def log_fetch_error(self, scope: RoutingScope, error: Exception) -> None:
        """Log a fetch error for a scope."""
        logger.warning(
            "Failed to fetch nodes for scope %s: %s",
            scope.name,
            error,
            extra={
                "event": "fetch_error",
                "scope": scope.name,
                "error_type": type(error).__name__,
            },
        )

    def log_all_scopes_failed(self) -> None:
        """Log that all scopes have been exhausted."""
        logger.error(
            "Failed to discover nodes from any scope",
            extra={"event": "discovery_failed"},
        )


def _uses_topology_filter(scope: RoutingScope) -> bool:
    from alternator.core.routing_scope import DatacenterScope, RackScope

    return isinstance(scope, DatacenterScope | RackScope)


def _uses_cluster_scope(scope: RoutingScope) -> bool:
    from alternator.core.routing_scope import ClusterScope

    return isinstance(scope, ClusterScope)


def _validate_topology_scope_values(scope: RoutingScope) -> None:
    from alternator.core.routing_scope import DatacenterScope, RackScope

    if isinstance(scope, RackScope):
        if not scope.datacenter or not scope.rack:
            raise ConfigurationError(
                "Rack routing requires non-empty datacenter and rack names"
            )
        return
    if isinstance(scope, DatacenterScope) and not scope.datacenter:
        raise ConfigurationError("Datacenter routing requires a non-empty datacenter")


def _no_nodes_for_scope_error(scope: RoutingScope) -> ConfigurationError:
    return ConfigurationError(f"No nodes found for routing scope: {scope.description}")


def _format_host_port(host: str, port: int) -> str:
    """Return a URL authority for a host-only address and shared port."""
    if host.startswith("[") and host.endswith("]"):
        return f"{host}:{port}"
    if _is_ipv6_literal(host):
        return f"[{host}]:{port}"
    return f"{host}:{port}"


def _is_ipv6_literal(host: str) -> bool:
    try:
        return ipaddress.ip_address(host).version == 6
    except ValueError:
        return False


class SyncLiveNodesManager:
    """
    Synchronous LiveNodesManager with background refresh thread.

    This manager handles:
    - Background thread for periodic node discovery
    - Fallback chain when scopes return empty results
    - Thread-safe node selection
    """

    def __init__(
        self,
        config: Config,
        http_fetch: SyncHttpFetcher,
    ) -> None:
        """
        Initialize the sync live nodes manager.

        Args:
            config: Alternator configuration
            http_fetch: HTTP fetcher that retrieves nodes from a URL
        """
        self._core = LiveNodesManagerCore(config)
        self._config = config
        self._http_fetch = http_fetch
        self._stop_event = threading.Event()
        self._refresh_thread: threading.Thread | None = None

    def start(self) -> None:
        """Start background refresh thread."""
        if self._refresh_thread is not None and self._refresh_thread.is_alive():
            return
        self._stop_event.clear()
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop,
            daemon=True,
            name="alternator-node-refresh",
        )
        self._refresh_thread.start()

    def stop(self) -> None:
        """Stop background refresh thread."""
        self._stop_event.set()
        if self._refresh_thread:
            self._refresh_thread.join(timeout=5.0)
            self._refresh_thread = None

    @property
    def nodes(self) -> NodeList:
        """Get current node list."""
        return self._core.nodes

    def next_node(self) -> str | None:
        """Get next node hostname using round-robin."""
        return self._core.next_node()

    def set_fallback_nodes(self, nodes: Sequence[str], scope: RoutingScope) -> None:
        """
        Set fallback nodes when initial discovery fails.

        Args:
            nodes: List of fallback node addresses
            scope: Routing scope to associate with the nodes
        """
        self._core.update_nodes(nodes, scope)

    def next_node_uri(self) -> str:
        """
        Get next node as full URI.

        Returns:
            Full URI with scheme, host, and port (e.g., "http://192.168.1.1:8000")

        Raises:
            NoNodesAvailableError: If no nodes are available
        """
        node = self._core.next_node()
        if not node:
            raise NoNodesAvailableError(
                "No nodes available for routing",
                scope_name=self._config.routing_scope.name,
                attempted_hosts=list(self._config.seed_hosts),
            )
        authority = _format_host_port(node, self._config.port)
        return f"{self._config.scheme}://{authority}"

    def refresh_nodes(self) -> bool:
        """
        Manually trigger a node refresh.

        Returns:
            True if nodes were successfully fetched, False otherwise
        """
        return self._refresh_nodes()

    def check_rack_datacenter_feature_supported(self) -> bool:
        """Report whether scoped rack/datacenter discovery appears supported."""
        scope = self._config.routing_scope
        if not _uses_topology_filter(scope):
            return True
        try:
            _validate_topology_scope_values(scope)
        except ConfigurationError:
            return False
        return bool(self._fetch_scope_nodes(scope))

    def check_rack_and_datacenter_set_correctly(self) -> bool:
        """Validate configured rack/datacenter scope without changing state."""
        scope = self._config.routing_scope
        if not _uses_topology_filter(scope):
            return True
        _validate_topology_scope_values(scope)
        if self._fetch_scope_nodes(scope):
            return True
        raise _no_nodes_for_scope_error(scope)

    def _refresh_loop(self) -> None:
        """Background thread that refreshes node list."""
        while not self._stop_event.is_set():
            with contextlib.suppress(Exception):
                self._refresh_nodes()

            interval = self._core.get_refresh_interval_seconds()
            self._stop_event.wait(timeout=interval)

    def _refresh_nodes(self) -> bool:
        """
        Fetch and update node list using fallback chain.

        Tries all seed hosts for each scope before falling back.

        Returns:
            True if nodes were successfully fetched
        """
        scope: RoutingScope | None = self._config.routing_scope

        while scope is not None:
            if _uses_cluster_scope(scope):
                if self._refresh_cluster_scope_nodes(scope):
                    return True
                scope = scope.fallback
                continue

            for seed in self._config.seed_hosts:
                url = self._core.build_localnodes_url(scope, seed)
                try:
                    nodes = self._http_fetch(url)
                    if self._core.process_fetch_result(nodes, scope):
                        return True
                except Exception as e:
                    self._core.log_fetch_error(scope, e)
            scope = scope.fallback

        self._core.log_all_scopes_failed()
        return False

    def _refresh_cluster_scope_nodes(self, scope: RoutingScope) -> bool:
        """Fetch cluster scope by aggregating local node lists from every seed."""
        discovered: list[str] = []
        seen: set[str] = set()

        for seed in self._config.seed_hosts:
            url = self._core.build_localnodes_url(scope, seed)
            try:
                nodes = self._http_fetch(url)
            except Exception as e:
                self._core.log_fetch_error(scope, e)
                continue

            for node in nodes:
                if node in seen:
                    continue
                seen.add(node)
                discovered.append(node)

        return self._core.process_fetch_result(discovered, scope)

    def _fetch_scope_nodes(self, scope: RoutingScope) -> list[str]:
        """Fetch nodes for one scope without updating current routing state."""
        for seed in self._config.seed_hosts:
            url = self._core.build_localnodes_url(scope, seed)
            try:
                nodes = list(self._http_fetch(url))
            except Exception as e:
                self._core.log_fetch_error(scope, e)
                continue
            if nodes:
                return nodes
        return []


class AsyncLiveNodesManager:
    """
    Asynchronous LiveNodesManager with background refresh task.

    This manager handles:
    - Background asyncio task for periodic node discovery
    - Fallback chain when scopes return empty results
    - Thread-safe node selection (works from any thread/task)
    """

    def __init__(
        self,
        config: Config,
        http_fetch: AsyncHttpFetcher,
    ) -> None:
        """
        Initialize the async live nodes manager.

        Args:
            config: Alternator configuration
            http_fetch: Async HTTP fetcher that retrieves nodes from a URL
        """
        self._core = LiveNodesManagerCore(config)
        self._config = config
        self._http_fetch = http_fetch
        self._refresh_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """Start background refresh task."""
        if self._refresh_task is not None and not self._refresh_task.done():
            return
        self._stop_event.clear()
        self._refresh_task = asyncio.create_task(
            self._refresh_loop(),
            name="alternator-node-refresh",
        )

    async def stop(self) -> None:
        """Stop background refresh task."""
        self._stop_event.set()
        if self._refresh_task:
            self._refresh_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._refresh_task
            self._refresh_task = None

    @property
    def nodes(self) -> NodeList:
        """Get current node list."""
        return self._core.nodes

    def next_node(self) -> str | None:
        """Get next node hostname using round-robin."""
        return self._core.next_node()

    def set_fallback_nodes(self, nodes: Sequence[str], scope: RoutingScope) -> None:
        """
        Set fallback nodes when initial discovery fails.

        Args:
            nodes: List of fallback node addresses
            scope: Routing scope to associate with the nodes
        """
        self._core.update_nodes(nodes, scope)

    def next_node_uri(self) -> str:
        """
        Get next node as full URI.

        Returns:
            Full URI with scheme, host, and port (e.g., "http://192.168.1.1:8000")

        Raises:
            NoNodesAvailableError: If no nodes are available
        """
        node = self._core.next_node()
        if not node:
            raise NoNodesAvailableError(
                "No nodes available for routing",
                scope_name=self._config.routing_scope.name,
                attempted_hosts=list(self._config.seed_hosts),
            )
        authority = _format_host_port(node, self._config.port)
        return f"{self._config.scheme}://{authority}"

    async def refresh_nodes(self) -> bool:
        """
        Manually trigger a node refresh.

        Returns:
            True if nodes were successfully fetched, False otherwise
        """
        return await self._refresh_nodes()

    async def check_rack_datacenter_feature_supported(self) -> bool:
        """Report whether scoped rack/datacenter discovery appears supported."""
        scope = self._config.routing_scope
        if not _uses_topology_filter(scope):
            return True
        try:
            _validate_topology_scope_values(scope)
        except ConfigurationError:
            return False
        return bool(await self._fetch_scope_nodes(scope))

    async def check_rack_and_datacenter_set_correctly(self) -> bool:
        """Validate configured rack/datacenter scope without changing state."""
        scope = self._config.routing_scope
        if not _uses_topology_filter(scope):
            return True
        _validate_topology_scope_values(scope)
        if await self._fetch_scope_nodes(scope):
            return True
        raise _no_nodes_for_scope_error(scope)

    async def _refresh_loop(self) -> None:
        """Background task that refreshes node list."""
        while not self._stop_event.is_set():
            with contextlib.suppress(Exception):
                await self._refresh_nodes()

            interval = self._core.get_refresh_interval_seconds()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=interval,
                )

    async def _refresh_nodes(self) -> bool:
        """
        Fetch and update node list using fallback chain.

        Tries all seed hosts for each scope before falling back.

        Returns:
            True if nodes were successfully fetched
        """
        scope: RoutingScope | None = self._config.routing_scope

        while scope is not None:
            if _uses_cluster_scope(scope):
                if await self._refresh_cluster_scope_nodes(scope):
                    return True
                scope = scope.fallback
                continue

            for seed in self._config.seed_hosts:
                url = self._core.build_localnodes_url(scope, seed)
                try:
                    nodes = await self._http_fetch(url)
                    if self._core.process_fetch_result(nodes, scope):
                        return True
                except Exception as e:
                    self._core.log_fetch_error(scope, e)
            scope = scope.fallback

        self._core.log_all_scopes_failed()
        return False

    async def _refresh_cluster_scope_nodes(self, scope: RoutingScope) -> bool:
        """Fetch cluster scope by aggregating local node lists from every seed."""
        discovered: list[str] = []
        seen: set[str] = set()

        for seed in self._config.seed_hosts:
            url = self._core.build_localnodes_url(scope, seed)
            try:
                nodes = await self._http_fetch(url)
            except Exception as e:
                self._core.log_fetch_error(scope, e)
                continue

            for node in nodes:
                if node in seen:
                    continue
                seen.add(node)
                discovered.append(node)

        return self._core.process_fetch_result(discovered, scope)

    async def _fetch_scope_nodes(self, scope: RoutingScope) -> list[str]:
        """Fetch nodes for one scope without updating current routing state."""
        for seed in self._config.seed_hosts:
            url = self._core.build_localnodes_url(scope, seed)
            try:
                nodes = list(await self._http_fetch(url))
            except Exception as e:
                self._core.log_fetch_error(scope, e)
                continue
            if nodes:
                return nodes
        return []
