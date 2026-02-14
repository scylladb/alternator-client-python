"""
Alternator Load Balancing Client for Python.

A library that provides client-side load balancing for ScyllaDB Alternator,
wrapping boto3/aioboto3 to transparently distribute requests across cluster nodes.

Quick Start
-----------
Synchronous usage::

    from alternator import AlternatorConfig, AlternatorClient

    config = AlternatorConfig(
        seed_hosts=["192.168.1.1", "192.168.1.2"],
        port=8000,
    )

    with AlternatorClient(config) as client:
        response = client.list_tables()
        client.put_item(
            TableName="my_table",
            Item={"pk": {"S": "user123"}, "data": {"S": "hello"}}
        )

Asynchronous usage::

    from alternator import AlternatorConfig
    from alternator.async_client import AsyncAlternatorClient

    config = AlternatorConfig(seed_hosts=["192.168.1.1"], port=8000)

    async with AsyncAlternatorClient(config) as client:
        response = await client.list_tables()

Configuration
-------------
Use ``AlternatorConfig`` for direct configuration or ``AlternatorConfigBuilder``
for a fluent builder pattern::

    from alternator import AlternatorConfigBuilder, CompressionAlgorithm

    config = (
        AlternatorConfigBuilder()
        .with_seeds("node1", "node2")
        .with_port(8000)
        .with_https()
        .with_compression(CompressionAlgorithm.GZIP)
        .build()
    )

Key Classes
-----------
- ``AlternatorClient``: Sync context manager for load-balanced connections
- ``AsyncAlternatorClient``: Async context manager for load-balanced connections
- ``AlternatorConfig``: Main configuration dataclass
- ``AlternatorConfigBuilder``: Fluent builder for configuration
- ``TlsConfig``: TLS/SSL configuration
- ``ClusterScope``, ``DatacenterScope``, ``RackScope``: Routing scope controls

Exceptions
----------
- ``AlternatorError``: Base exception for all Alternator errors
- ``NoNodesAvailableError``: Raised when no nodes are available for routing
- ``ConfigurationError``: Raised for invalid configuration

Notes
-----
- Gzip compression requires ScyllaDB 2026.1.0 or later
- For async support, install with: ``pip install alternator[async]``
"""

from alternator._version import __version__
from alternator.client import (
    AlternatorClient,
    AlternatorResource,
    close_client,
    close_resource,
    create_client,
    create_resource,
)
from alternator.config import (
    AlternatorConfig,
    AlternatorConfigBuilder,
    CompressionAlgorithm,
    KeyRouteAffinityConfig,
    KeyRouteAffinityMode,
    TlsConfig,
    TlsSessionCacheConfig,
)
from alternator.core.routing_scope import (
    ClusterScope,
    DatacenterScope,
    RackScope,
    RoutingScope,
)
from alternator.exceptions import (
    AlternatorError,
    ConfigurationError,
    NoNodesAvailableError,
)

__all__ = [
    # Version
    "__version__",
    # Sync Client
    "AlternatorClient",
    "AlternatorResource",
    "close_client",
    "close_resource",
    "create_client",
    "create_resource",
    # Async Client (requires [async] extra)
    "AsyncAlternatorClient",
    "close_async_client",
    "create_async_client",
    # Config
    "AlternatorConfig",
    "AlternatorConfigBuilder",
    "CompressionAlgorithm",
    "KeyRouteAffinityConfig",
    "KeyRouteAffinityMode",
    "TlsConfig",
    "TlsSessionCacheConfig",
    # Exceptions
    "AlternatorError",
    "ConfigurationError",
    "NoNodesAvailableError",
    # Routing Scopes
    "ClusterScope",
    "DatacenterScope",
    "RackScope",
    "RoutingScope",
]


def __getattr__(name: str) -> object:
    """Lazy import async client components to avoid requiring async dependencies."""
    if name in ("AsyncAlternatorClient", "close_async_client", "create_async_client"):
        from alternator import async_client

        return getattr(async_client, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Include lazy-imported async symbols in dir() output."""
    return list(__all__)
