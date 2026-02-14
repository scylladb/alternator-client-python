"""Live nodes management and selection for Alternator load balancing."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from alternator._http import AsyncHttpFetcher, SyncHttpFetcher
from alternator.exceptions import NoNodesAvailableError

if TYPE_CHECKING:
    from alternator.config import AlternatorConfig
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

    def __init__(self, config: AlternatorConfig) -> None:
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
        base = f"{self._config.scheme}://{seed}:{self._config.port}/localnodes"
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
        config: AlternatorConfig,
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

    @property
    def nodes(self) -> NodeList:
        """Get current node list."""
        return self._core.nodes

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
        return f"{self._config.scheme}://{node}:{self._config.port}"

    def refresh_nodes(self) -> bool:
        """
        Manually trigger a node refresh.

        Returns:
            True if nodes were successfully fetched, False otherwise
        """
        return self._refresh_nodes()

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
        config: AlternatorConfig,
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
        return f"{self._config.scheme}://{node}:{self._config.port}"

    async def refresh_nodes(self) -> bool:
        """
        Manually trigger a node refresh.

        Returns:
            True if nodes were successfully fetched, False otherwise
        """
        return await self._refresh_nodes()

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
