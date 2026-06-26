"""Asynchronous client factory for Alternator load balancing."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from types import TracebackType
from typing import TYPE_CHECKING, Any, cast

from alternator._constants import (
    MANAGER_ATTR,
    PK_CACHE_ATTR,
    PK_DISCOVERY_TIMEOUT_SECONDS,
)
from alternator._http import (
    AsyncNodeFetcher,
    create_async_http_fetcher,
    create_ssl_context,
)
from alternator.config import KeyRouteAffinityMode
from alternator.core.handlers import _register_alternator_handlers
from alternator.core.hashing import hash_attribute_value
from alternator.core.key_affinity import (
    extract_partition_key,
    get_table_name,
    should_use_affinity,
)
from alternator.core.live_nodes import AsyncLiveNodesManager
from alternator.vector import enable_vector_support

if TYPE_CHECKING:
    from types_aiobotocore_dynamodb import DynamoDBClient as AsyncDynamoDBClient

    from alternator.config import Config

logger = logging.getLogger("alternator")


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


def _create_async_affinity_hash_computer(
    config: Config,
    pk_cache: AsyncPartitionKeyCache | None,
) -> Callable[[str, dict[str, Any]], int | None] | None:
    """
    Create a function that computes the partition key hash for affinity routing.

    Args:
        config: Alternator configuration
        pk_cache: Partition key cache (if affinity enabled)

    Returns:
        Function that computes partition key hash, or None if affinity is disabled
    """
    affinity_mode = config.key_affinity.mode

    # If no affinity, return None (no hash computation)
    if affinity_mode == KeyRouteAffinityMode.NONE or pk_cache is None:
        return None

    def compute_affinity_hash(
        operation_name: str, params: dict[str, Any]
    ) -> int | None:
        """Compute partition key hash for affinity routing."""
        # Check if this operation should use affinity
        if not should_use_affinity(affinity_mode.name, operation_name, params):
            return None

        # Get table name
        table_name = get_table_name(params)
        if not table_name:
            return None

        # Get partition key name from cache (fast sync path)
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

        # Extract partition key value
        pk_info = extract_partition_key(params, pk_name)
        if not pk_info:
            logger.debug(
                "Could not extract partition key %s from request",
                pk_name,
            )
            return None

        attr_type, value = pk_info

        # Compute and return hash
        try:
            return hash_attribute_value(attr_type, value)
        except (ValueError, TypeError, UnicodeEncodeError) as e:
            logger.debug("Error hashing partition key: %s", e)
            return None

    return compute_affinity_hash


async def create_async_client(
    config: Config,
    **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
) -> AsyncDynamoDBClient:
    """
    Create a load-balanced async DynamoDB client for Alternator.

    The returned client is an aioboto3 DynamoDB client that
    transparently distributes requests across cluster nodes.

    Note:
        When ``optimize_headers`` is enabled, authentication headers are
        only preserved if credentials are passed explicitly via
        ``aws_access_key_id`` in *boto_kwargs*. Unlike a regular boto3
        DynamoDB client, credentials from environment variables or
        ``~/.aws/credentials`` are not detected for this purpose.

    Args:
        config: Alternator configuration
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
    try:
        import aioboto3
        from aiobotocore.config import AioConfig
    except ImportError as e:
        raise ImportError(
            "aioboto3 is required for async support. "
            "Install with: pip install alternator[async]"
        ) from e

    # Create SSL context if using HTTPS
    ssl_context = None
    if config.scheme == "https":
        ssl_context = create_ssl_context(config.tls)

    # Create async HTTP fetcher for /localnodes
    http_fetcher = create_async_http_fetcher(
        ssl_context, timeout_seconds=config.timeouts.discovery_seconds
    )

    # Create and start async live nodes manager
    manager = AsyncLiveNodesManager(config, http_fetcher)

    # Perform initial node fetch
    if not await manager.refresh_nodes():
        logger.warning("Initial node discovery failed, using seed hosts")
        from alternator.core.routing_scope import ClusterScope

        manager.set_fallback_nodes(list(config.seed_hosts), ClusterScope())

    # Start background refresh task
    await manager.start()

    # Get initial endpoint
    initial_endpoint = manager.next_node_uri()

    # BotoConfig is managed internally — don't let callers override it
    boto_kwargs.pop("config", None)

    # Create boto config from Config settings
    from botocore import UNSIGNED

    auth_enabled = "aws_access_key_id" in boto_kwargs
    aio_kwargs: dict[str, Any] = {
        "retries": {
            "max_attempts": config.retries.max_attempts,
            "mode": config.retries.mode.value,
        },
        "max_pool_connections": config.max_pool_connections,
    }
    if not auth_enabled:
        aio_kwargs["signature_version"] = UNSIGNED
    boto_config = AioConfig(**aio_kwargs)

    # Create aioboto3 session and client
    # Alternator doesn't use AWS regions, but boto3 requires one;
    # default to "us-east-1" unless the caller overrides it.
    boto_kwargs.setdefault("region_name", "us-east-1")
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
            _create_async_affinity_hash_computer(config, pk_cache),
            auth_enabled=auth_enabled,
        )

        # Attach manager for cleanup reference
        setattr(client, MANAGER_ATTR, manager)

        # Enable Alternator vector search extensions
        enable_vector_support(client)
    except Exception:
        # Clean up the HTTP fetcher session on failure
        if isinstance(http_fetcher, AsyncNodeFetcher):
            await http_fetcher.close()
        else:
            http_close = getattr(http_fetcher, "close", None)
            if http_close is not None:
                result = http_close()
                if inspect.isawaitable(result):
                    await result
        await manager.stop()
        await client_ctx.__aexit__(None, None, None)
        raise

    return cast("AsyncDynamoDBClient", client)


async def close_async_client(client: AsyncDynamoDBClient) -> None:
    """
    Close an async Alternator client and stop its background refresh task.

    Args:
        client: Client created by create_async_client
    """
    try:
        manager = getattr(client, MANAGER_ATTR, None)
        if manager is not None:
            await manager.stop()
            # Close the HTTP fetcher session if it has a close method
            http_fetch = manager._http_fetch
            if isinstance(http_fetch, AsyncNodeFetcher):
                await http_fetch.close()
            else:
                http_close = getattr(http_fetch, "close", None)
                if http_close is not None:
                    result = http_close()
                    if inspect.isawaitable(result):
                        await result
            setattr(client, MANAGER_ATTR, None)

        # Clear PK cache reference
        if hasattr(client, PK_CACHE_ATTR):
            setattr(client, PK_CACHE_ATTR, None)
    finally:
        # Always close the underlying client
        await client.__aexit__(None, None, None)


class AsyncAlternatorClient:
    """
    Async context manager for load-balanced Alternator connections.

    Handles proper cleanup of background refresh tasks.

    Example:
        async with AsyncAlternatorClient(config) as client:
            await client.put_item(...)
    """

    def __init__(self, config: Config, **boto_kwargs: Any) -> None:  # noqa: ANN401 -- boto3 kwargs are untyped
        self._config = config
        self._boto_kwargs = boto_kwargs
        self._client: AsyncDynamoDBClient | None = None

    async def __aenter__(self) -> AsyncDynamoDBClient:
        self._client = await create_async_client(self._config, **self._boto_kwargs)
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
