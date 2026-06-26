"""Asynchronous client factory for Alternator load balancing."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from collections.abc import Callable
from types import TracebackType
from typing import TYPE_CHECKING, Any, cast

from alternator._constants import (
    MANAGER_ATTR,
    MANAGER_OWNS_ATTR,
    PK_CACHE_ATTR,
    PK_DISCOVERY_TIMEOUT_SECONDS,
)
from alternator._http import (
    AsyncNodeFetcher,
    create_async_http_fetcher,
    create_ssl_context,
)
from alternator.config import KeyRouteAffinityMode, build_sdk_config_kwargs
from alternator.core.auth import apply_auth
from alternator.core.handlers import _register_alternator_handlers
from alternator.core.key_affinity import (
    select_affinity_node,
)
from alternator.core.live_nodes import AsyncLiveNodesManager, NodeList
from alternator.vector import enable_vector_support

if TYPE_CHECKING:
    from types_aiobotocore_dynamodb import DynamoDBClient as AsyncDynamoDBClient

    from alternator.config import Auth, Config

logger = logging.getLogger("alternator")
_AUTH_UNSET = object()


class AsyncPartitionKeyCache:
    """
    Async version of partition key cache for table -> pk name mapping.

    Uses async DescribeTable calls for auto-discovery.
    Thread-safe using asyncio.Lock to protect concurrent access.
    Uses a pending-state pattern to avoid duplicate DescribeTable calls.
    """

    def __init__(self, client: AsyncDynamoDBClient) -> None:
        """
        Initialize the cache.

        Args:
            client: aioboto3 DynamoDB client for DescribeTable calls
        """
        self._client = client
        self._cache: dict[str, str] = {}
        self._pending: dict[str, asyncio.Event] = {}
        self._errors: dict[str, Exception] = {}
        self._lock = asyncio.Lock()

    async def get_pk_name(self, table_name: str) -> str | None:
        """
        Get partition key name for a table, using cache when available.

        Uses a pending-state pattern to avoid duplicate DescribeTable calls
        for concurrent requests to the same table.

        Args:
            table_name: Name of the DynamoDB table

        Returns:
            Partition key attribute name, or None if not found
        """
        # Fast path: check cache without lock
        if table_name in self._cache:
            return self._cache[table_name]

        should_fetch = False
        event: asyncio.Event | None = None

        async with self._lock:
            # Double-check cache after acquiring lock
            if table_name in self._cache:
                return self._cache[table_name]

            # Check if a fetch is already in progress
            if table_name in self._pending:
                event = self._pending[table_name]
            else:
                # We are the first - create event and mark for fetch
                event = asyncio.Event()
                self._pending[table_name] = event
                should_fetch = True

        if should_fetch:
            # We are responsible for fetching
            try:
                pk_name = await self._fetch_pk_name(table_name)
                async with self._lock:
                    if pk_name:
                        self._cache[table_name] = pk_name
                    self._pending.pop(table_name, None)
                    self._errors.pop(table_name, None)
            except Exception as exc:
                async with self._lock:
                    self._pending.pop(table_name, None)
                    self._errors[table_name] = exc
                logger.warning(
                    "Failed to fetch partition key for table %s: %s",
                    table_name,
                    exc,
                    extra={
                        "event": "pk_fetch_error",
                        "table": table_name,
                    },
                )
            finally:
                if event:
                    event.set()
            return self._cache.get(table_name)

        # Wait for in-progress fetch to complete (with timeout to avoid deadlock)
        if event:
            try:
                await asyncio.wait_for(
                    event.wait(), timeout=PK_DISCOVERY_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Timed out waiting for partition key discovery for table %s",
                    table_name,
                    extra={
                        "event": "pk_discovery_timeout",
                        "table": table_name,
                    },
                )
                return None

        # Check if the fetcher encountered an error
        async with self._lock:
            error = self._errors.pop(table_name, None)
        if error is not None:
            logger.debug(
                "Partition key fetch for table %s failed previously: %s",
                table_name,
                error,
            )
            return None

        return self._cache.get(table_name)

    async def _fetch_pk_name(self, table_name: str) -> str | None:
        """
        Fetch partition key name from DescribeTable.

        Args:
            table_name: Name of the DynamoDB table

        Returns:
            Partition key attribute name, or None if not found
        """
        try:
            response = await self._client.describe_table(TableName=table_name)
            key_schema = response.get("Table", {}).get("KeySchema", [])
            for key in key_schema:
                if key.get("KeyType") == "HASH":
                    attr_name = key.get("AttributeName")
                    return str(attr_name) if attr_name else None
        except Exception as e:
            logger.warning(
                "Failed to describe table %s for partition key discovery: %s",
                table_name,
                e,
                extra={
                    "event": "describe_table_failed",
                    "table": table_name,
                    "error_type": type(e).__name__,
                },
            )
        return None

    async def clear(self) -> None:
        """Clear the cache."""
        async with self._lock:
            self._cache.clear()

    def preload(self, table_pk_map: dict[str, str]) -> None:
        """
        Preload cache with known table -> pk mappings.

        Note: This is synchronous and intended to be called during setup
        before concurrent access begins.

        Args:
            table_pk_map: Mapping of table name to partition key name
        """
        self._cache.update(table_pk_map)


def _create_async_affinity_node_computer(
    config: Config,
    pk_cache: AsyncPartitionKeyCache | None,
) -> Callable[[str, dict[str, Any], NodeList], str | None] | None:
    """
    Create a function that selects the preferred key-affinity node.

    Args:
        config: Alternator configuration
        pk_cache: Partition key cache (if affinity enabled)

    Returns:
        Function that selects the affinity node, or None if affinity is disabled
    """
    affinity_mode = config.key_affinity.mode

    # If no affinity, return None (no hash computation)
    if affinity_mode == KeyRouteAffinityMode.NONE or pk_cache is None:
        return None

    def get_cached_pk_name(table_name: str) -> str | None:
        pk_name = pk_cache._cache.get(table_name)
        if not pk_name:
            # Schedule async discovery for future requests
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(pk_cache.get_pk_name(table_name))
            except RuntimeError:
                pass
            logger.debug(
                "Partition key for table %s not yet cached, scheduled async discovery",
                table_name,
            )
            return None
        return pk_name

    def compute_affinity_node(
        operation_name: str,
        params: dict[str, Any],
        nodes: NodeList,
    ) -> str | None:
        """Select the preferred key-affinity node for this request."""
        return select_affinity_node(
            mode=affinity_mode.name,
            operation_name=operation_name,
            params=params,
            nodes=nodes,
            get_pk_name=get_cached_pk_name,
        )

    return compute_affinity_node


async def _create_async_manager(
    config: Config,
    *,
    initial_refresh: bool = True,
) -> AsyncLiveNodesManager:
    """Create and initialize an async live-node manager."""
    # Create SSL context if using HTTPS
    ssl_context = None
    if config.scheme == "https":
        ssl_context = create_ssl_context(config.tls)

    # Create async HTTP fetcher for /localnodes
    http_fetcher = create_async_http_fetcher(
        ssl_context, timeout_seconds=config.timeouts.discovery_seconds
    )

    # Create async live nodes manager
    manager = AsyncLiveNodesManager(config, http_fetcher)

    # Perform initial node fetch
    if initial_refresh and not await manager.refresh_nodes():
        from alternator.core.routing_scope import (
            ClusterScope,
            scope_chain_includes_cluster,
        )

        if scope_chain_includes_cluster(config.routing_scope):
            logger.warning("Initial node discovery failed, using seed hosts")
            manager.set_fallback_nodes(list(config.seed_hosts), ClusterScope())

    return manager


async def _close_async_manager(manager: AsyncLiveNodesManager) -> None:
    """Stop an async manager and close its discovery fetcher."""
    await manager.stop()
    http_fetch = manager._http_fetch
    if isinstance(http_fetch, AsyncNodeFetcher):
        await http_fetch.close()
        return

    http_close = getattr(http_fetch, "close", None)
    if http_close is not None:
        result = http_close()
        if inspect.isawaitable(result):
            await result


def _create_aio_config(config: Config, *, auth_enabled: bool) -> object:
    """Create aiobotocore AioConfig from Config settings."""
    try:
        from aiobotocore.config import AioConfig
    except ImportError as e:
        raise ImportError(
            "aiobotocore is required for async support. "
            "Install with: pip install alternator[async]"
        ) from e

    from botocore import UNSIGNED

    kwargs = build_sdk_config_kwargs(config)
    kwargs.pop("signature_version", None)
    if not auth_enabled:
        kwargs["signature_version"] = UNSIGNED
    return AioConfig(**kwargs)


async def _create_async_client_with_manager(
    config: Config,
    manager: AsyncLiveNodesManager,
    *,
    auth: Auth | None = None,
    owns_manager: bool,
    **boto_kwargs: Any,  # noqa: ANN401 -- aioboto3 kwargs are untyped
) -> AsyncDynamoDBClient:
    """Create an aioboto3 client using an already initialized manager."""
    try:
        import aioboto3
    except ImportError as e:
        raise ImportError(
            "aioboto3 is required for async support. "
            "Install with: pip install alternator[async]"
        ) from e

    # Get initial endpoint
    initial_endpoint = manager.next_node_uri()

    # BotoConfig is managed internally — don't let callers override it
    boto_kwargs.pop("config", None)

    auth_enabled = apply_auth(auth, boto_kwargs)
    boto_config = _create_aio_config(config, auth_enabled=auth_enabled)

    # Create aioboto3 session and client
    # Alternator doesn't use AWS regions, but boto3 requires one;
    # default to "us-east-1" unless the caller overrides it.
    boto_kwargs.setdefault("region_name", config.aws_region)
    session = aioboto3.Session()
    client_ctx = session.client(
        "dynamodb",
        endpoint_url=initial_endpoint,
        config=boto_config,
        **boto_kwargs,
    )
    client = await client_ctx.__aenter__()

    try:
        # Set up partition key cache if affinity is enabled
        pk_cache: AsyncPartitionKeyCache | None = None
        if config.key_affinity.mode != KeyRouteAffinityMode.NONE:
            pk_cache = AsyncPartitionKeyCache(client)
            if config.key_affinity.table_pk_attributes:
                pk_cache.preload(dict(config.key_affinity.table_pk_attributes))
            setattr(client, PK_CACHE_ATTR, pk_cache)

        # Register all event handlers (endpoint routing, compression, headers)
        _register_alternator_handlers(
            client.meta.events,
            manager,
            config,
            _create_async_affinity_node_computer(config, pk_cache),
            auth_enabled=auth_enabled,
        )

        # Attach manager for cleanup reference
        setattr(client, MANAGER_ATTR, manager)
        setattr(client, MANAGER_OWNS_ATTR, owns_manager)

        # Enable Alternator vector search extensions
        enable_vector_support(client)
    except Exception:
        await client_ctx.__aexit__(None, None, None)
        raise

    return cast("AsyncDynamoDBClient", client)


async def create_async_client(
    config: Config,
    *,
    auth: Auth | None = None,
    **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
) -> AsyncDynamoDBClient:
    """
    Create a load-balanced async DynamoDB client for Alternator.

    The returned client is an aioboto3 DynamoDB client that
    transparently distributes requests across cluster nodes.

    Note:
        Authentication is disabled by default. Alternator authentication
        currently supports only static credentials; pass
        ``auth=Auth.static_credentials(...)`` to enable request signing.

    Args:
        config: Alternator configuration
        auth: Explicit Alternator auth settings. Defaults to disabled auth.
        **boto_kwargs: Additional arguments passed to aioboto3.client()

    Returns:
        An async DynamoDB client with load balancing enabled

    Example:
        from alternator import Config
        from alternator.async_client import create_async_client

        config = Config(
            seed_hosts=["node1.example.com"],
            port=8000,
        )
        client = await create_async_client(config)

        # Use like a normal aioboto3 client
        response = await client.list_tables()
    """
    manager = await _create_async_manager(config)
    await manager.start()
    try:
        return await _create_async_client_with_manager(
            config,
            manager,
            auth=auth,
            owns_manager=True,
            **boto_kwargs,
        )
    except Exception:
        await _close_async_manager(manager)
        raise


async def close_async_client(client: AsyncDynamoDBClient) -> None:
    """
    Close an async Alternator client and stop its background refresh task.

    Args:
        client: Client created by create_async_client
    """
    try:
        manager = getattr(client, MANAGER_ATTR, None)
        if manager is not None:
            owns_manager = bool(getattr(client, MANAGER_OWNS_ATTR, True))
            if owns_manager:
                await _close_async_manager(manager)
            setattr(client, MANAGER_ATTR, None)
            setattr(client, MANAGER_OWNS_ATTR, False)

        # Clear PK cache reference
        if hasattr(client, PK_CACHE_ATTR):
            setattr(client, PK_CACHE_ATTR, None)
    finally:
        # Always close the underlying client
        await client.__aexit__(None, None, None)


class AsyncHelper:
    """
    Async facade for Alternator client lifecycle and diagnostics.

    The helper owns one async live-node manager and can create standard
    aioboto3 DynamoDB clients that share that discovery state.
    """

    def __init__(
        self,
        config: Config,
        *,
        auth: Auth | None = None,
        **boto_kwargs: Any,  # noqa: ANN401 -- aioboto3 kwargs are untyped
    ) -> None:
        self._config = config
        self._auth = auth
        self._boto_kwargs = dict(boto_kwargs)
        self._manager: AsyncLiveNodesManager | None = None
        self._clients: list[AsyncDynamoDBClient] = []

    async def __aenter__(self) -> AsyncHelper:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.stop()

    @property
    def config(self) -> Config:
        """Return this helper's configuration."""
        return self._config

    def update(
        self,
        config: Config | None = None,
        *,
        auth: Auth | None | object = _AUTH_UNSET,
        **boto_kwargs: Any,  # noqa: ANN401 -- aioboto3 kwargs are untyped
    ) -> AsyncHelper:
        """Return a new async helper with updated config, auth, or boto kwargs."""
        next_auth = self._auth if auth is _AUTH_UNSET else cast("Auth | None", auth)
        next_kwargs = {**self._boto_kwargs, **boto_kwargs}
        return type(self)(config or self._config, auth=next_auth, **next_kwargs)

    async def start(self) -> AsyncHelper:
        """Start background node discovery."""
        await (await self._ensure_manager()).start()
        return self

    async def stop(self) -> None:
        """Stop background node discovery and close helper-created clients."""
        for created_client in list(self._clients):
            with contextlib.suppress(Exception):
                await close_async_client(created_client)
        self._clients.clear()

        if self._manager is not None:
            await _close_async_manager(self._manager)
            self._manager = None

    async def client(
        self,
        **boto_kwargs: Any,  # noqa: ANN401 -- aioboto3 kwargs are untyped
    ) -> AsyncDynamoDBClient:
        """Create a standard aioboto3 DynamoDB client using this helper."""
        manager = await self._ensure_manager()
        await manager.start()
        client = await _create_async_client_with_manager(
            self._config,
            manager,
            auth=self._auth,
            owns_manager=False,
            **{**self._boto_kwargs, **boto_kwargs},
        )
        self._clients.append(client)
        return client

    async def update_live_nodes(self) -> bool:
        """Refresh the live-node list immediately."""
        return await (await self._ensure_manager()).refresh_nodes()

    async def next_node(self) -> str | None:
        """Return the next live node selected for diagnostics."""
        return (await self._ensure_manager()).next_node()

    def get_nodes(self) -> list[str]:
        """Return the current live-node hostnames."""
        if self._manager is None:
            return []
        return list(self._manager.nodes.nodes)

    def get_active_nodes(self) -> list[str]:
        """Return active nodes; currently this is the live-node list."""
        return self.get_nodes()

    def get_quarantined_nodes(self) -> list[str]:
        """Return quarantined nodes; node quarantine is not implemented."""
        return []

    async def check_rack_and_datacenter_set_correctly(self) -> bool:
        """Validate configured rack/datacenter scope without changing state."""
        manager = self._manager
        if manager is not None:
            return await manager.check_rack_and_datacenter_set_correctly()

        manager = await _create_async_manager(self._config, initial_refresh=False)
        try:
            return await manager.check_rack_and_datacenter_set_correctly()
        finally:
            await _close_async_manager(manager)

    async def check_rack_datacenter_feature_supported(self) -> bool:
        """Report whether scoped rack/datacenter discovery appears supported."""
        manager = self._manager
        if manager is not None:
            return await manager.check_rack_datacenter_feature_supported()

        manager = await _create_async_manager(self._config, initial_refresh=False)
        try:
            return await manager.check_rack_datacenter_feature_supported()
        finally:
            await _close_async_manager(manager)

    async def get_partition_key_name(self, table_name: str) -> str | None:
        """Return a known partition key name for diagnostics."""
        configured = self._config.key_affinity.table_pk_attributes.get(table_name)
        if configured is not None:
            return configured

        for created_client in self._clients:
            pk_cache = getattr(created_client, PK_CACHE_ATTR, None)
            if pk_cache is None:
                continue
            get_pk_name = getattr(pk_cache, "get_pk_name", None)
            if not callable(get_pk_name):
                continue
            result = get_pk_name(table_name)
            if inspect.isawaitable(result):
                return cast("str | None", await result)
            return cast("str | None", result)
        return None

    async def _ensure_manager(self) -> AsyncLiveNodesManager:
        if self._manager is None:
            self._manager = await _create_async_manager(self._config)
        return self._manager


class AsyncAlternatorClient:
    """
    Async context manager for load-balanced Alternator connections.

    Handles proper cleanup of background refresh tasks.

    Example:
        async with AsyncAlternatorClient(config) as client:
            await client.put_item(...)
    """

    def __init__(
        self,
        config: Config,
        *,
        auth: Auth | None = None,
        **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
    ) -> None:
        self._config = config
        self._auth = auth
        self._boto_kwargs = boto_kwargs
        self._client: AsyncDynamoDBClient | None = None

    async def __aenter__(self) -> AsyncDynamoDBClient:
        self._client = await create_async_client(
            self._config, auth=self._auth, **self._boto_kwargs
        )
        return self._client

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    @property
    def client(self) -> AsyncDynamoDBClient:
        """Access the underlying aioboto3 client."""
        if self._client is None:
            raise RuntimeError("Client not initialized. Use as async context manager.")
        return self._client

    async def close(self) -> None:
        """Stop background tasks and release resources."""
        if self._client is not None:
            await close_async_client(self._client)
            self._client = None
