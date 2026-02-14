"""Synchronous client factory for Alternator load balancing."""

from __future__ import annotations

import atexit
import contextlib
import logging
import threading
import weakref
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config as BotoConfig

from alternator._constants import MANAGER_ATTR, PK_CACHE_ATTR
from alternator._http import create_ssl_context, create_sync_http_fetcher
from alternator.config import KeyRouteAffinityMode
from alternator.core.execution_plan import create_execution_plan_factory
from alternator.core.handlers import register_alternator_handlers
from alternator.core.hashing import hash_attribute_value
from alternator.core.key_affinity import (
    PartitionKeyCache,
    extract_partition_key,
    get_table_name,
    should_use_affinity,
)
from alternator.core.live_nodes import SyncLiveNodesManager

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource

    from alternator.config import AlternatorConfig

logger = logging.getLogger("alternator")

# Registry of active managers for cleanup on exit
_active_managers: weakref.WeakValueDictionary[int, SyncLiveNodesManager] = (
    weakref.WeakValueDictionary()
)
_registry_lock = threading.Lock()


def _cleanup_manager(manager_id: int) -> None:
    """Callback to stop manager when client is garbage collected.

    This may run during interpreter shutdown when modules are partially
    torn down, so all operations are wrapped in a broad try/except.
    """
    try:
        with _registry_lock:
            manager = _active_managers.pop(manager_id, None)
        if manager is not None:
            manager.stop()
            logger.debug(
                "Stopped manager via garbage collection cleanup",
                extra={"event": "manager_gc_cleanup", "manager_id": manager_id},
            )
    except Exception:
        pass


def _register_manager(manager: SyncLiveNodesManager, client: Any) -> None:
    """Register a manager for cleanup tracking and set up GC finalizer."""
    manager_id = id(manager)
    with _registry_lock:
        _active_managers[manager_id] = manager
    weakref.finalize(client, _cleanup_manager, manager_id)


@atexit.register
def _cleanup_all_managers() -> None:
    """Stop all active managers on program exit."""
    with _registry_lock:
        managers = list(_active_managers.values())
        _active_managers.clear()

    for manager in managers:
        with contextlib.suppress(Exception):
            manager.stop()


def _create_and_start_manager(config: AlternatorConfig) -> SyncLiveNodesManager:
    """
    Create and initialize a SyncLiveNodesManager.

    This handles:
    - SSL context creation for HTTPS
    - HTTP fetcher setup
    - Initial node discovery with fallback
    - Starting background refresh thread

    Args:
        config: Alternator configuration

    Returns:
        Initialized and running SyncLiveNodesManager
    """
    # Create SSL context if using HTTPS
    ssl_context = None
    if config.scheme == "https":
        ssl_context = create_ssl_context(config.tls)

    # Create HTTP fetcher for /localnodes
    http_fetcher = create_sync_http_fetcher(
        ssl_context, timeout_seconds=config.discovery_timeout_seconds
    )

    # Create and start live nodes manager
    manager = SyncLiveNodesManager(config, http_fetcher)

    # Perform initial node fetch (blocking)
    if not manager.refresh_nodes():
        logger.warning("Initial node discovery failed, using seed hosts")
        from alternator.core.routing_scope import ClusterScope

        manager.set_fallback_nodes(list(config.seed_hosts), ClusterScope())

    # Start background refresh thread
    manager.start()

    return manager


def _get_merged_boto_config(
    config: AlternatorConfig,
    boto_config: BotoConfig | None,
) -> BotoConfig:
    """
    Merge user-provided boto config with Alternator defaults.

    Args:
        config: Alternator configuration
        boto_config: Optional user-provided BotoConfig

    Returns:
        Merged BotoConfig with pool connection settings
    """
    if boto_config is None:
        return BotoConfig(
            retries={"max_attempts": 3, "mode": "standard"},
            max_pool_connections=config.max_pool_connections,
        )
    if "max_pool_connections" not in boto_config._user_provided_options:
        return boto_config.merge(
            BotoConfig(max_pool_connections=config.max_pool_connections)
        )
    return boto_config


def _create_affinity_hash_computer(
    config: AlternatorConfig,
    client: Any,
) -> Callable[[str, dict[str, Any]], int | None] | None:
    """
    Create a function that computes the partition key hash for affinity routing.

    Args:
        config: Alternator configuration
        client: boto3 client for DescribeTable calls

    Returns:
        Function that computes partition key hash, or None if affinity is disabled
    """
    affinity_mode = config.key_affinity.mode

    # If no affinity, return None (no hash computation)
    if affinity_mode == KeyRouteAffinityMode.NONE:
        return None

    # Set up partition key cache
    pk_cache = PartitionKeyCache(client)

    # Preload any configured PK mappings
    if config.key_affinity.table_pk_attributes:
        pk_cache.preload(dict(config.key_affinity.table_pk_attributes))

    # Store pk_cache on client for cleanup/access
    setattr(client, PK_CACHE_ATTR, pk_cache)

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

        # Get partition key name (from config or auto-discover)
        pk_name = pk_cache.get_pk_name(table_name)
        if not pk_name:
            logger.debug(
                "Could not determine partition key for table %s",
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
            logger.error("Error hashing partition key: %s", e)
            return None

    return compute_affinity_hash


def create_client(
    config: AlternatorConfig,
    **boto_kwargs: Any,
) -> DynamoDBClient:
    """
    Create a load-balanced DynamoDB client for Alternator.

    The returned client is a standard boto3 DynamoDB client that
    transparently distributes requests across cluster nodes.

    Args:
        config: Alternator configuration
        **boto_kwargs: Additional arguments passed to boto3.client()

    Returns:
        A DynamoDB client with load balancing enabled

    Example:
        from alternator import AlternatorConfig, create_client

        config = AlternatorConfig(
            seed_hosts=["node1.example.com"],
            port=8000,
        )
        client = create_client(config)

        # Use like a normal boto3 client
        response = client.list_tables()
    """
    # Create and start manager (handles SSL, HTTP fetcher, node discovery)
    manager = _create_and_start_manager(config)

    # Get initial endpoint and merged boto config
    initial_endpoint = manager.next_node_uri()
    boto_config = _get_merged_boto_config(config, boto_kwargs.pop("config", None))

    # Create boto3 client
    client: DynamoDBClient = boto3.client(
        "dynamodb",
        endpoint_url=initial_endpoint,
        config=boto_config,
        **boto_kwargs,
    )

    # Register manager for cleanup tracking (with GC finalizer on client)
    _register_manager(manager, client)

    # Create execution plan factory and affinity hash computer
    create_plan = create_execution_plan_factory(config, manager)
    compute_affinity_hash = _create_affinity_hash_computer(config, client)

    # Register all handlers
    register_alternator_handlers(
        client.meta.events, create_plan, config, compute_affinity_hash
    )

    # Attach manager for cleanup reference
    setattr(client, MANAGER_ATTR, manager)

    return client


def create_resource(
    config: AlternatorConfig,
    **boto_kwargs: Any,
) -> DynamoDBServiceResource:
    """
    Create a load-balanced DynamoDB resource for Alternator.

    Example:
        resource = create_resource(config)
        table = resource.Table("my_table")
        table.put_item(Item={"pk": "123", "data": "hello"})
    """
    # Create and start manager (handles SSL, HTTP fetcher, node discovery)
    manager = _create_and_start_manager(config)

    # Get initial endpoint and merged boto config
    initial_endpoint = manager.next_node_uri()
    boto_config = _get_merged_boto_config(config, boto_kwargs.pop("config", None))

    # Create boto3 resource
    resource: DynamoDBServiceResource = boto3.resource(
        "dynamodb",
        endpoint_url=initial_endpoint,
        config=boto_config,
        **boto_kwargs,
    )

    # Register manager for cleanup tracking (with GC finalizer on resource)
    _register_manager(manager, resource)

    # Create execution plan factory and affinity hash computer
    create_plan = create_execution_plan_factory(config, manager)
    compute_affinity_hash = _create_affinity_hash_computer(config, resource.meta.client)

    # Register all handlers
    register_alternator_handlers(
        resource.meta.client.meta.events,
        create_plan,
        config,
        compute_affinity_hash,
    )

    # Attach manager for cleanup reference
    setattr(resource, MANAGER_ATTR, manager)

    return resource


def close_client(client: DynamoDBClient | DynamoDBServiceResource) -> None:
    """
    Close an Alternator client or resource and stop its background refresh thread.

    Works for both clients created by ``create_client`` and resources created
    by ``create_resource``. See also ``close_resource`` alias.

    Args:
        client: Client or resource created by create_client or create_resource
    """
    manager = getattr(client, MANAGER_ATTR, None)
    if manager is not None:
        # Remove from registry first
        manager_id = id(manager)
        with _registry_lock:
            _active_managers.pop(manager_id, None)

        manager.stop()
        setattr(client, MANAGER_ATTR, None)

    # Clear PK cache reference
    if hasattr(client, PK_CACHE_ATTR):
        setattr(client, PK_CACHE_ATTR, None)


class AlternatorClient:
    """
    Context manager for load-balanced Alternator connections.

    Handles proper cleanup of background refresh threads.

    Example:
        with AlternatorClient(config) as client:
            client.put_item(...)
    """

    def __init__(self, config: AlternatorConfig, **boto_kwargs: Any) -> None:
        self._config = config
        self._boto_kwargs = boto_kwargs
        self._client: DynamoDBClient | None = None

    def __enter__(self) -> DynamoDBClient:
        self._client = create_client(self._config, **self._boto_kwargs)
        return self._client

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.close()

    @property
    def client(self) -> DynamoDBClient:
        """Access the underlying boto3 client."""
        if self._client is None:
            raise RuntimeError("Client not initialized. Use as context manager.")
        return self._client

    def close(self) -> None:
        """Stop background threads and release resources."""
        if self._client is not None:
            close_client(self._client)
            self._client = None


class AlternatorResource:
    """
    Context manager for load-balanced Alternator resource connections.

    Handles proper cleanup of background refresh threads.

    Example:
        with AlternatorResource(config) as resource:
            table = resource.Table("my_table")
            table.put_item(Item={"pk": "123", "data": "hello"})
    """

    def __init__(self, config: AlternatorConfig, **boto_kwargs: Any) -> None:
        self._config = config
        self._boto_kwargs = boto_kwargs
        self._resource: DynamoDBServiceResource | None = None

    def __enter__(self) -> DynamoDBServiceResource:
        self._resource = create_resource(self._config, **self._boto_kwargs)
        return self._resource

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.close()

    @property
    def resource(self) -> DynamoDBServiceResource:
        """Access the underlying boto3 resource."""
        if self._resource is None:
            raise RuntimeError("Resource not initialized. Use as context manager.")
        return self._resource

    def close(self) -> None:
        """Stop background threads and release resources."""
        if self._resource is not None:
            close_client(self._resource)
            self._resource = None


# Alias for close_client that makes intent clearer when closing resources
close_resource = close_client
