"""
Alternator Load Balancing Client for Python.

A library that provides client-side load balancing for ScyllaDB Alternator,
wrapping boto3/aioboto3 to transparently distribute requests across cluster nodes.

Quick Start
-----------
Synchronous usage::

    import alternator

    with alternator.client(
        "dynamodb",
        seeds=["192.168.1.1", "192.168.1.2"],
        port=8000,
    ) as client:
        response = client.list_tables()
        client.put_item(
            TableName="my_table",
            Item={"pk": {"S": "user123"}, "data": {"S": "hello"}}
        )

Asynchronous usage::

    from alternator import Config
    from alternator.async_client import AsyncSession

    config = Config(seed_hosts=["192.168.1.1"], port=8000)

    async with AsyncSession(config) as session:
        client = await session.client("dynamodb")
        response = await client.list_tables()

Configuration
-------------
Use ``Config`` for direct configuration::

    from alternator import Config, CompressionAlgorithm, RequestCompressionConfig

    config = Config(
        seed_hosts=["node1", "node2"],
        port=8000,
        scheme="https",
        request_compression=RequestCompressionConfig(
            algorithm=CompressionAlgorithm.GZIP,
        ),
    )

Key Classes
-----------
- ``client("dynamodb", ...)``: Sync context manager for load-balanced connections
- ``resource("dynamodb", ...)``: Sync context manager for DynamoDB resources
- ``Session``: Sync lifecycle and diagnostics facade
- ``AsyncSession``: Async lifecycle and diagnostics facade
- ``Config``: Main configuration dataclass
- ``Auth``: Explicit disabled/static-credentials auth settings
- ``TLS``: TLS/SSL configuration
- ``ClusterScope``, ``DatacenterScope``, ``RackScope``: Routing scope controls

Exceptions
----------
- ``AlternatorError``: Base exception for all Alternator errors
- ``NoNodesAvailableError``: Raised when no nodes are available for routing
- ``ConfigurationError``: Raised for invalid configuration

Vector Search (ScyllaDB Extension)
-----------------------------------
- ``Vector``: Optimized vector type stored as ``FLOAT32VECTOR`` on the wire

Notes
-----
- Gzip compression requires ScyllaDB 2026.1.0 or later
- Response compression supports gzip and deflate when the server supports it
- For async support, install with: ``pip install alternator[async]``
- Vector search is a ScyllaDB Alternator extension not available on AWS DynamoDB
"""

from alternator._version import __version__
from alternator.client import (
    Session,
    client,
    resource,
)
from alternator.config import (
    TLS,
    Auth,
    CompressionAlgorithm,
    Config,
    HeaderOptimizationConfig,
    HeaderWhitelistCallback,
    HeaderWhitelistContext,
    KeyRouteAffinityConfig,
    KeyRouteAffinityMode,
    NodeListPollingConfig,
    RequestCompressionConfig,
    ResponseCompression,
    RetryConfig,
    RetryMode,
    TimeoutConfig,
    UserAgent,
    UserAgentCustomizer,
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
from alternator.vector import Vector

__all__ = [
    # Version
    "__version__",
    # Sync Client
    "Session",
    "client",
    "resource",
    # Async Client (requires [async] extra)
    "AsyncSession",
    # Config
    "Auth",
    "Config",
    "TLS",
    "CompressionAlgorithm",
    "HeaderOptimizationConfig",
    "HeaderWhitelistCallback",
    "HeaderWhitelistContext",
    "RequestCompressionConfig",
    "ResponseCompression",
    "KeyRouteAffinityConfig",
    "KeyRouteAffinityMode",
    "NodeListPollingConfig",
    "RetryConfig",
    "RetryMode",
    "TimeoutConfig",
    "UserAgent",
    "UserAgentCustomizer",
    # Exceptions
    "AlternatorError",
    "ConfigurationError",
    "NoNodesAvailableError",
    # Routing Scopes
    "ClusterScope",
    "DatacenterScope",
    "RackScope",
    "RoutingScope",
    # Vector Search (ScyllaDB extension)
    "Vector",
]


def __getattr__(name: str) -> object:
    """Lazy import async client components to avoid requiring async dependencies."""
    if name in ("AsyncSession",):
        from alternator import async_client

        return getattr(async_client, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Include lazy-imported async symbols in dir() output."""
    return list(__all__)
