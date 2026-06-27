"""Synchronous client factory for Alternator load balancing."""

from __future__ import annotations

import atexit
import contextlib
import logging
import threading
import weakref
from collections.abc import Callable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, cast

import boto3
from botocore.config import Config as BotoConfig

from alternator._constants import MANAGER_ATTR, MANAGER_OWNS_ATTR, PK_CACHE_ATTR
from alternator._http import create_ssl_context, create_sync_http_fetcher
from alternator.config import Config, KeyRouteAffinityMode, build_sdk_config_kwargs
from alternator.core.auth import apply_auth
from alternator.core.handlers import _register_alternator_handlers
from alternator.core.key_affinity import (
    PartitionKeyCache,
    select_affinity_node,
)
from alternator.core.live_nodes import NodeList, SyncLiveNodesManager
from alternator.exceptions import ConfigurationError
from alternator.vector import enable_vector_support

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource

    from alternator.config import Auth

logger = logging.getLogger("alternator")
DEFAULT_PORT = 8000

# Registry of active managers for cleanup on exit
_active_managers: weakref.WeakValueDictionary[int, SyncLiveNodesManager] = (
    weakref.WeakValueDictionary()
)
_registry_lock = threading.Lock()
_AUTH_UNSET = object()


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


def _register_active_manager(manager: SyncLiveNodesManager) -> None:
    """Register a manager for process-exit cleanup."""
    with _registry_lock:
        _active_managers[id(manager)] = manager


def _unregister_active_manager(manager: SyncLiveNodesManager) -> None:
    """Remove a manager from process-exit cleanup tracking."""
    with _registry_lock:
        _active_managers.pop(id(manager), None)


def _register_manager(
    manager: SyncLiveNodesManager, client: DynamoDBClient | DynamoDBServiceResource
) -> None:
    """Register a manager for cleanup tracking and set up GC finalizer."""
    manager_id = id(manager)
    _register_active_manager(manager)
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


def _create_manager(
    config: Config,
    *,
    initial_refresh: bool = True,
) -> SyncLiveNodesManager:
    """
    Create and initialize a SyncLiveNodesManager.

    This handles:
    - SSL context creation for HTTPS
    - HTTP fetcher setup
    - Initial node discovery with fallback

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
        ssl_context, timeout_seconds=config.timeouts.discovery_seconds
    )

    # Create and start live nodes manager
    manager = SyncLiveNodesManager(config, http_fetcher)

    # Perform initial node fetch (blocking)
    if initial_refresh and not manager.refresh_nodes():
        from alternator.core.routing_scope import (
            ClusterScope,
            scope_chain_includes_cluster,
        )

        if scope_chain_includes_cluster(config.routing_scope):
            logger.warning("Initial node discovery failed, using seed hosts")
            manager.set_fallback_nodes(list(config.seed_hosts), ClusterScope())

    return manager


def _create_and_start_manager(config: Config) -> SyncLiveNodesManager:
    """Create a manager and start its background refresh thread."""
    manager = _create_manager(config)
    manager.start()
    return manager


def _create_boto_config(config: Config, *, auth_enabled: bool) -> BotoConfig:
    """Create BotoConfig from Config settings.

    When no credentials are provided (auth_enabled=False), uses UNSIGNED
    signature to skip request signing entirely.
    """
    from botocore import UNSIGNED

    kwargs = build_sdk_config_kwargs(config)
    kwargs.pop("signature_version", None)
    if not auth_enabled:
        kwargs["signature_version"] = UNSIGNED
    return BotoConfig(**kwargs)


def _create_affinity_node_computer(
    config: Config,
    client: DynamoDBClient,
) -> Callable[[str, dict[str, Any], NodeList], str | None] | None:
    """
    Create a function that selects the preferred key-affinity node.

    Args:
        config: Alternator configuration
        client: boto3 client for DescribeTable calls

    Returns:
        Function that selects the affinity node, or None if affinity is disabled
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
            get_pk_name=pk_cache.get_pk_name,
        )

    return compute_affinity_node


def _create_client_with_manager(
    config: Config,
    manager: SyncLiveNodesManager,
    *,
    auth: Auth | None = None,
    owns_manager: bool,
    **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
) -> DynamoDBClient:
    """Create a boto3 client using an already initialized manager."""
    # BotoConfig is managed internally — don't let callers override it
    boto_kwargs.pop("config", None)

    # Get initial endpoint and merged boto config
    initial_endpoint = manager.next_node_uri()
    auth_enabled = apply_auth(auth, boto_kwargs)
    boto_config = _create_boto_config(config, auth_enabled=auth_enabled)
    user_agent = cast("str | None", getattr(boto_config, "user_agent", None))

    # Create boto3 client
    # Alternator doesn't use AWS regions, but boto3 requires one;
    # default to "us-east-1" unless the caller overrides it.
    boto_kwargs.setdefault("region_name", config.aws_region)
    client: DynamoDBClient = boto3.client(
        "dynamodb",
        endpoint_url=initial_endpoint,
        config=boto_config,
        **boto_kwargs,
    )

    # Register all handlers
    _register_alternator_handlers(
        client.meta.events,
        manager,
        config,
        _create_affinity_node_computer(config, client),
        auth_enabled=auth_enabled,
        user_agent=user_agent,
    )

    # Attach manager for cleanup reference
    setattr(client, MANAGER_ATTR, manager)
    setattr(client, MANAGER_OWNS_ATTR, owns_manager)

    # Enable Alternator vector search extensions before registering finalizers.
    enable_vector_support(client)

    if owns_manager:
        _register_manager(manager, client)

    return client


def create_client(
    config: Config,
    *,
    auth: Auth | None = None,
    **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
) -> DynamoDBClient:
    """
    Create a load-balanced DynamoDB client for Alternator.

    The returned client is a standard boto3 DynamoDB client that
    transparently distributes requests across cluster nodes.

    Note:
        Authentication is disabled by default. Alternator authentication
        currently supports only static credentials; pass
        ``auth=Auth.static_credentials(...)`` to enable request signing.

    Args:
        config: Alternator configuration
        auth: Explicit Alternator auth settings. Defaults to disabled auth.
        **boto_kwargs: Additional arguments passed to boto3.client()
            (excluding ``config`` — BotoConfig is managed internally)

    Returns:
        A DynamoDB client with load balancing enabled

    Example:
        from alternator import Config, create_client

        config = Config(
            seed_hosts=["node1.example.com"],
            port=8000,
        )
        client = create_client(config)

        # Use like a normal boto3 client
        response = client.list_tables()
    """
    manager = _create_and_start_manager(config)
    try:
        return _create_client_with_manager(
            config,
            manager,
            auth=auth,
            owns_manager=True,
            **boto_kwargs,
        )
    except Exception:
        manager.stop()
        _unregister_active_manager(manager)
        raise


def _seed_has_port(seed: str) -> bool:
    """Return whether a seed appears to include a port."""
    if "://" in seed:
        return True
    if seed.startswith("["):
        return "]:" in seed
    if seed.count(":") == 1:
        _, maybe_port = seed.rsplit(":", 1)
        return maybe_port.isdigit()
    return False


def _validate_host_only_seeds(seed_hosts: Sequence[str]) -> None:
    invalid = [seed for seed in seed_hosts if _seed_has_port(seed)]
    if invalid:
        raise ConfigurationError(
            "seeds must be host names or IP addresses without ports; "
            "use the port argument for the single Alternator port"
        )


def _validate_service_name(service_name: str) -> None:
    """Validate the boto3-style service name accepted by this client."""
    if service_name != "dynamodb":
        raise ConfigurationError(
            f"alternator only supports the 'dynamodb' service, got {service_name!r}"
        )


def _config_from_client_args(
    cluster_config: Config | None,
    *,
    seeds: Sequence[str] | None,
    port: int,
    scheme: Literal["http", "https"],
) -> Config:
    """Build or validate an Alternator config from boto3-style factory args."""
    if cluster_config is not None:
        if seeds is not None or port != DEFAULT_PORT or scheme != "http":
            raise ConfigurationError(
                "Do not combine cluster_config with seeds, port, or scheme"
            )
        config = cluster_config
    else:
        if seeds is None:
            raise ConfigurationError(
                "seeds is required when cluster_config is not provided"
            )
        _validate_host_only_seeds(seeds)
        config = Config(seed_hosts=tuple(seeds), port=port, scheme=scheme)

    _validate_host_only_seeds(config.seed_hosts)
    return config


def client(
    service_name: str,
    *,
    cluster_config: Config | None = None,
    seeds: Sequence[str] | None = None,
    port: int = DEFAULT_PORT,
    scheme: Literal["http", "https"] = "http",
    auth: Auth | None = None,
    **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
) -> AlternatorClient:
    """
    Create a context-manager friendly Alternator client.

    Mirrors ``boto3.client("dynamodb", ...)`` while adding Alternator cluster
    discovery options. Seeds are host names or IP addresses only; provide the
    shared Alternator port with ``port``.
    """
    _validate_service_name(service_name)
    config = _config_from_client_args(
        cluster_config,
        seeds=seeds,
        port=port,
        scheme=scheme,
    )
    return AlternatorClient(config, auth=auth, **boto_kwargs)


def resource(
    service_name: str,
    *,
    cluster_config: Config | None = None,
    seeds: Sequence[str] | None = None,
    port: int = DEFAULT_PORT,
    scheme: Literal["http", "https"] = "http",
    auth: Auth | None = None,
    **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
) -> AlternatorResource:
    """
    Create a context-manager friendly Alternator resource.

    Mirrors ``boto3.resource("dynamodb", ...)`` for callers that use boto3's
    high-level DynamoDB table API.
    """
    _validate_service_name(service_name)
    config = _config_from_client_args(
        cluster_config,
        seeds=seeds,
        port=port,
        scheme=scheme,
    )
    return AlternatorResource(config, auth=auth, **boto_kwargs)


def create_resource(
    config: Config,
    *,
    auth: Auth | None = None,
    **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
) -> DynamoDBServiceResource:
    """
    Create a load-balanced DynamoDB resource for Alternator.

    Note:
        Authentication is disabled by default. Alternator authentication
        currently supports only static credentials; pass
        ``auth=Auth.static_credentials(...)`` to enable request signing.

    Example:
        resource = create_resource(config)
        table = resource.Table("my_table")
        table.put_item(Item={"pk": "123", "data": "hello"})
    """
    manager = _create_and_start_manager(config)
    try:
        return _create_resource_with_manager(
            config,
            manager,
            auth=auth,
            owns_manager=True,
            **boto_kwargs,
        )
    except Exception:
        manager.stop()
        _unregister_active_manager(manager)
        raise


def _create_resource_with_manager(
    config: Config,
    manager: SyncLiveNodesManager,
    *,
    auth: Auth | None = None,
    owns_manager: bool,
    **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
) -> DynamoDBServiceResource:
    """Create a boto3 resource using an already initialized manager."""
    # BotoConfig is managed internally — don't let callers override it
    boto_kwargs.pop("config", None)

    # Get initial endpoint and merged boto config
    initial_endpoint = manager.next_node_uri()
    auth_enabled = apply_auth(auth, boto_kwargs)
    boto_config = _create_boto_config(config, auth_enabled=auth_enabled)
    user_agent = cast("str | None", getattr(boto_config, "user_agent", None))

    # Create boto3 resource
    # Alternator doesn't use AWS regions, but boto3 requires one;
    # default to "us-east-1" unless the caller overrides it.
    boto_kwargs.setdefault("region_name", config.aws_region)
    resource: DynamoDBServiceResource = boto3.resource(
        "dynamodb",
        endpoint_url=initial_endpoint,
        config=boto_config,
        **boto_kwargs,
    )

    # Register all handlers
    _register_alternator_handlers(
        resource.meta.client.meta.events,
        manager,
        config,
        _create_affinity_node_computer(config, resource.meta.client),
        auth_enabled=auth_enabled,
        user_agent=user_agent,
    )

    # Attach manager for cleanup reference
    setattr(resource, MANAGER_ATTR, manager)
    setattr(resource, MANAGER_OWNS_ATTR, owns_manager)

    # Enable Alternator vector search extensions before registering finalizers.
    enable_vector_support(resource)

    if owns_manager:
        _register_manager(manager, resource)

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
        owns_manager = bool(getattr(client, MANAGER_OWNS_ATTR, True))
        if owns_manager:
            _unregister_active_manager(manager)
            manager.stop()
        setattr(client, MANAGER_ATTR, None)
        setattr(client, MANAGER_OWNS_ATTR, False)

    # Clear PK cache reference
    if hasattr(client, PK_CACHE_ATTR):
        setattr(client, PK_CACHE_ATTR, None)
    meta = getattr(client, "meta", None)
    service_client = getattr(meta, "client", None)
    if service_client is not None and hasattr(service_client, PK_CACHE_ATTR):
        setattr(service_client, PK_CACHE_ATTR, None)

    sdk_close = getattr(client, "close", None)
    if callable(sdk_close):
        sdk_close()
        return

    service_client_close = getattr(service_client, "close", None)
    if callable(service_client_close):
        service_client_close()


class Session:
    """
    Public session for Alternator client lifecycle and diagnostics.

    The session owns one live-node manager and can create standard boto3
    DynamoDB clients and resources that share that discovery state.
    """

    def __init__(
        self,
        cluster_config: Config | None = None,
        *,
        seeds: Sequence[str] | None = None,
        port: int = DEFAULT_PORT,
        scheme: Literal["http", "https"] = "http",
        auth: Auth | None = None,
        **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
    ) -> None:
        self._config = _config_from_client_args(
            cluster_config,
            seeds=seeds,
            port=port,
            scheme=scheme,
        )
        self._auth = auth
        self._boto_kwargs = dict(boto_kwargs)
        self._manager: SyncLiveNodesManager | None = None
        self._manager_finalizer: Any | None = None
        self._clients: list[DynamoDBClient | DynamoDBServiceResource] = []

    def __enter__(self) -> Session:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()

    @property
    def config(self) -> Config:
        """Return this session's configuration."""
        return self._config

    def update(
        self,
        cluster_config: Config | None = None,
        *,
        auth: Auth | None | object = _AUTH_UNSET,
        **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
    ) -> Session:
        """Return a new session with updated config, auth, or boto kwargs."""
        next_auth = self._auth if auth is _AUTH_UNSET else cast("Auth | None", auth)
        next_kwargs = {**self._boto_kwargs, **boto_kwargs}
        return type(self)(
            cluster_config or self._config,
            auth=next_auth,
            **next_kwargs,
        )

    def start(self) -> Session:
        """Start background node discovery."""
        self._ensure_manager().start()
        return self

    def stop(self) -> None:
        """Stop background node discovery and detach session-created clients."""
        for created_client in list(self._clients):
            with contextlib.suppress(Exception):
                close_client(created_client)
        self._clients.clear()

        if self._manager is not None:
            _unregister_active_manager(self._manager)
            self._manager.stop()
            self._manager = None

        if self._manager_finalizer is not None:
            self._manager_finalizer.detach()
            self._manager_finalizer = None

    def client(
        self,
        service_name: str,
        **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
    ) -> DynamoDBClient:
        """Create a standard boto3 DynamoDB client using this session."""
        _validate_service_name(service_name)
        manager = self._ensure_manager()
        manager.start()
        client = _create_client_with_manager(
            self._config,
            manager,
            auth=self._auth,
            owns_manager=False,
            **{**self._boto_kwargs, **boto_kwargs},
        )
        self._clients.append(client)
        return client

    def resource(
        self,
        service_name: str,
        **boto_kwargs: Any,  # noqa: ANN401 -- boto3 kwargs are untyped
    ) -> DynamoDBServiceResource:
        """Create a standard boto3 DynamoDB resource using this session."""
        _validate_service_name(service_name)
        manager = self._ensure_manager()
        manager.start()
        resource = _create_resource_with_manager(
            self._config,
            manager,
            auth=self._auth,
            owns_manager=False,
            **{**self._boto_kwargs, **boto_kwargs},
        )
        self._clients.append(resource)
        return resource

    def refresh_nodes(self) -> bool:
        """Refresh the live-node list immediately."""
        return self._ensure_manager().refresh_nodes()

    @property
    def nodes(self) -> list[str]:
        """Return the current live-node hostnames."""
        if self._manager is None:
            return []
        return list(self._manager.nodes.nodes)

    @property
    def active_nodes(self) -> list[str]:
        """Return active nodes; currently this is the live-node list."""
        return self.nodes

    @property
    def quarantined_nodes(self) -> list[str]:
        """Return quarantined nodes; node quarantine is not implemented."""
        return []

    def validate_scope(self) -> bool:
        """Return whether the configured rack/datacenter scope is complete."""
        manager = self._manager
        if manager is not None:
            return manager.check_rack_and_datacenter_set_correctly()

        manager = _create_manager(self._config, initial_refresh=False)
        try:
            return manager.check_rack_and_datacenter_set_correctly()
        finally:
            manager.stop()

    def supports_topology_filters(self) -> bool:
        """Return whether this client supports rack/datacenter scoped discovery."""
        manager = self._manager
        if manager is not None:
            return manager.check_rack_datacenter_feature_supported()

        manager = _create_manager(self._config, initial_refresh=False)
        try:
            return manager.check_rack_datacenter_feature_supported()
        finally:
            manager.stop()

    def partition_key_for(self, table_name: str) -> str | None:
        """Return a known partition key name for diagnostics."""
        configured = self._config.key_affinity.table_pk_attributes.get(table_name)
        if configured is not None:
            return configured

        for created_client in self._clients:
            pk_cache = self._get_partition_key_cache(created_client)
            if pk_cache is None:
                continue
            get_pk_name = getattr(pk_cache, "get_pk_name", None)
            if callable(get_pk_name):
                return cast("str | None", get_pk_name(table_name))
        return None

    def _ensure_manager(self) -> SyncLiveNodesManager:
        if self._manager is None:
            manager = _create_manager(self._config)
            _register_active_manager(manager)
            self._manager_finalizer = weakref.finalize(
                self,
                _cleanup_manager,
                id(manager),
            )
            self._manager = manager
        return self._manager

    def _get_partition_key_cache(
        self,
        created_client: DynamoDBClient | DynamoDBServiceResource,
    ) -> object | None:
        pk_cache = getattr(created_client, PK_CACHE_ATTR, None)
        if pk_cache is not None:
            return cast(object, pk_cache)

        meta = getattr(created_client, "meta", None)
        service_client = getattr(meta, "client", None)
        return cast(object | None, getattr(service_client, PK_CACHE_ATTR, None))


class AlternatorClient:
    """
    Context manager for load-balanced Alternator connections.

    Handles proper cleanup of background refresh threads.

    Example:
        with AlternatorClient(config) as client:
            client.put_item(...)
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
        self._client: DynamoDBClient | None = None

    def __enter__(self) -> DynamoDBClient:
        self._client = create_client(self._config, auth=self._auth, **self._boto_kwargs)
        return self._client

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
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
        self._resource: DynamoDBServiceResource | None = None

    def __enter__(self) -> DynamoDBServiceResource:
        self._resource = create_resource(
            self._config, auth=self._auth, **self._boto_kwargs
        )
        return self._resource

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
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
