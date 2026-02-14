"""Configuration classes for Alternator load balancing client."""

from __future__ import annotations

import logging
import os
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING

from alternator.exceptions import ConfigurationError

logger = logging.getLogger("alternator")

if TYPE_CHECKING:
    from alternator.core.routing_scope import RoutingScope


class CompressionAlgorithm(Enum):
    """Compression algorithm for request bodies."""

    NONE = auto()
    GZIP = auto()


class KeyRouteAffinityMode(Enum):
    """Mode for key-based routing affinity (LWT optimization)."""

    NONE = auto()  # Default round-robin
    RMW = auto()  # Read-modify-write operations only
    ANY_WRITE = auto()  # All write operations


@dataclass(frozen=True)
class TlsSessionCacheConfig:
    """
    TLS session cache settings for connection reuse.

    Note:
        Python's ``ssl`` module does not expose direct control over session
        cache size or timeout. Only the ``enabled`` flag is effective, controlling
        whether session tickets (OP_NO_TICKET) are enabled or disabled.
        The ``cache_size`` and ``timeout_seconds`` parameters are reserved for
        future use with custom implementations or alternative TLS backends.
    """

    enabled: bool = True
    cache_size: int = 1024  # Reserved for future use
    timeout_seconds: int = 86400  # Reserved for future use (24 hours)


@dataclass(frozen=True)
class TlsConfig:
    """TLS/SSL configuration for HTTPS connections."""

    # Certificate trust settings
    custom_ca_cert_paths: Sequence[Path] = field(default_factory=tuple)
    trust_system_ca_certs: bool = True
    trust_all_certificates: bool = False  # INSECURE - dev only

    # Verification settings
    verify_hostname: bool = True

    # Session caching
    session_cache: TlsSessionCacheConfig = field(default_factory=TlsSessionCacheConfig)

    @classmethod
    def trust_all(cls) -> TlsConfig:
        """
        Create insecure config that trusts all certificates.

        .. warning::
            **SECURITY WARNING**: This configuration disables all TLS certificate
            verification and should NEVER be used in production. It makes connections
            vulnerable to man-in-the-middle attacks.

            This is intended ONLY for:
            - Local development with self-signed certificates
            - Testing environments

            To suppress this warning, set the environment variable
            ``ALTERNATOR_ALLOW_INSECURE_TLS=1``.
        """
        msg = (
            "TlsConfig.trust_all() disables TLS certificate verification. "
            "This is INSECURE and should only be used for development. "
            "Set ALTERNATOR_ALLOW_INSECURE_TLS=1 to suppress this warning."
        )
        # Always log at WARNING so it appears in production logs
        logger.warning(msg)
        if not os.environ.get("ALTERNATOR_ALLOW_INSECURE_TLS"):
            warnings.warn(msg, UserWarning, stacklevel=2)
        return cls(
            trust_all_certificates=True,
            verify_hostname=False,
        )

    @classmethod
    def system_default(cls) -> TlsConfig:
        """Create config using system CA certificates."""
        return cls(trust_system_ca_certs=True)

    @classmethod
    def with_custom_ca(cls, *cert_paths: Path) -> TlsConfig:
        """Create config with custom CA certificates."""
        return cls(custom_ca_cert_paths=tuple(cert_paths))


@dataclass(frozen=True)
class KeyRouteAffinityConfig:
    """Configuration for LWT-optimized routing."""

    mode: KeyRouteAffinityMode = KeyRouteAffinityMode.NONE

    # Table name -> partition key attribute name mapping
    # If not provided, will auto-discover via DescribeTable
    table_pk_attributes: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class AlternatorConfig:
    """
    Main configuration for Alternator load balancing client.

    Example:
        config = AlternatorConfig(
            seed_hosts=["192.168.1.1", "192.168.1.2"],
            port=8000,
        )
    """

    # Required: Initial nodes for discovery
    seed_hosts: Sequence[str]

    # Required: Connection settings
    port: int
    scheme: str = "http"  # "http" or "https"

    # Routing scope for topology awareness
    routing_scope: RoutingScope = field(
        default_factory=lambda: _default_cluster_scope()
    )

    # Compression settings
    compression: CompressionAlgorithm = CompressionAlgorithm.NONE
    min_compression_size_bytes: int = 1024

    # Header optimization
    optimize_headers: bool = False
    headers_whitelist: frozenset[str] | None = None

    # Authentication
    authentication_enabled: bool = True

    # TLS configuration (used when scheme="https")
    tls: TlsConfig = field(default_factory=TlsConfig.system_default)

    # Key route affinity for LWT optimization
    key_affinity: KeyRouteAffinityConfig = field(default_factory=KeyRouteAffinityConfig)

    # Connection pooling
    max_pool_connections: int = 200  # Max connections per host

    # Refresh intervals (milliseconds)
    active_refresh_interval_ms: int = 1000  # 1 second
    idle_refresh_interval_ms: int = 60000  # 1 minute

    # Timeout settings (seconds)
    discovery_timeout_seconds: float = 5.0  # Timeout for /localnodes requests
    connect_timeout_seconds: float = 5.0  # TCP connection timeout
    read_timeout_seconds: float = 30.0  # Read timeout for operations

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if not self.seed_hosts:
            raise ConfigurationError("At least one seed host is required")
        if self.scheme not in ("http", "https"):
            raise ConfigurationError(
                f"scheme must be 'http' or 'https', got {self.scheme!r}"
            )
        if self.port <= 0 or self.port > 65535:
            raise ConfigurationError(f"port must be 1-65535, got {self.port}")
        if self.min_compression_size_bytes < 0:
            raise ConfigurationError(
                f"min_compression_size_bytes must be >= 0, got {self.min_compression_size_bytes}"
            )
        if self.max_pool_connections <= 0:
            raise ConfigurationError(
                f"max_pool_connections must be > 0, got {self.max_pool_connections}"
            )
        if self.active_refresh_interval_ms <= 0:
            raise ConfigurationError(
                f"active_refresh_interval_ms must be > 0, got {self.active_refresh_interval_ms}"
            )
        if self.idle_refresh_interval_ms <= 0:
            raise ConfigurationError(
                f"idle_refresh_interval_ms must be > 0, got {self.idle_refresh_interval_ms}"
            )
        if self.discovery_timeout_seconds <= 0:
            raise ConfigurationError(
                f"discovery_timeout_seconds must be > 0, got {self.discovery_timeout_seconds}"
            )
        if self.connect_timeout_seconds <= 0:
            raise ConfigurationError(
                f"connect_timeout_seconds must be > 0, got {self.connect_timeout_seconds}"
            )
        if self.read_timeout_seconds <= 0:
            raise ConfigurationError(
                f"read_timeout_seconds must be > 0, got {self.read_timeout_seconds}"
            )


def _default_cluster_scope() -> RoutingScope:
    """Create default ClusterScope (avoids circular import)."""
    from alternator.core.routing_scope import ClusterScope

    return ClusterScope()


class AlternatorConfigBuilder:
    """
    Fluent builder for AlternatorConfig.

    Example:
        config = (
            AlternatorConfigBuilder()
            .with_seeds("192.168.1.1", "192.168.1.2")
            .with_port(8000)
            .with_https()
            .with_datacenter("dc1")
            .with_compression(CompressionAlgorithm.GZIP)
            .build()
        )
    """

    def __init__(self) -> None:
        self._seed_hosts: list[str] = []
        self._port: int = 8000
        self._scheme: str = "http"
        self._routing_scope: RoutingScope | None = None
        self._compression = CompressionAlgorithm.NONE
        self._min_compression_size = 1024
        self._optimize_headers = False
        self._headers_whitelist: frozenset[str] | None = None
        self._auth_enabled = True
        self._tls = TlsConfig.system_default()
        self._key_affinity = KeyRouteAffinityConfig()
        self._max_pool_connections = 200
        self._active_refresh_ms = 1000
        self._idle_refresh_ms = 60000
        self._discovery_timeout = 5.0
        self._connect_timeout = 5.0
        self._read_timeout = 30.0

    def with_seeds(self, *hosts: str) -> AlternatorConfigBuilder:
        """Add seed hosts for node discovery."""
        self._seed_hosts.extend(hosts)
        return self

    def with_port(self, port: int) -> AlternatorConfigBuilder:
        """Set the Alternator port."""
        self._port = port
        return self

    def with_http(self) -> AlternatorConfigBuilder:
        """Use HTTP scheme (default)."""
        self._scheme = "http"
        return self

    def with_https(
        self, tls_config: TlsConfig | None = None
    ) -> AlternatorConfigBuilder:
        """Use HTTPS scheme with optional TLS configuration."""
        self._scheme = "https"
        if tls_config:
            self._tls = tls_config
        return self

    def with_cluster_scope(self) -> AlternatorConfigBuilder:
        """Route to any node in the cluster (default)."""
        from alternator.core.routing_scope import ClusterScope

        self._routing_scope = ClusterScope()
        return self

    def with_datacenter(self, dc: str) -> AlternatorConfigBuilder:
        """Route to nodes in a specific datacenter."""
        from alternator.core.routing_scope import DatacenterScope

        self._routing_scope = DatacenterScope(datacenter=dc)
        return self

    def with_rack(self, dc: str, rack: str) -> AlternatorConfigBuilder:
        """Route to nodes in a specific rack within a datacenter."""
        from alternator.core.routing_scope import RackScope

        self._routing_scope = RackScope(datacenter=dc, rack=rack)
        return self

    def with_compression(
        self, algorithm: CompressionAlgorithm, min_size: int = 1024
    ) -> AlternatorConfigBuilder:
        """Enable request compression."""
        self._compression = algorithm
        self._min_compression_size = min_size
        return self

    def with_header_optimization(
        self, whitelist: frozenset[str] | set[str] | None = None
    ) -> AlternatorConfigBuilder:
        """Enable header optimization (filtering)."""
        self._optimize_headers = True
        self._headers_whitelist = (
            frozenset(whitelist) if whitelist is not None else None
        )
        return self

    def with_key_affinity(
        self,
        mode: KeyRouteAffinityMode,
        table_pk_map: Mapping[str, str] | None = None,
    ) -> AlternatorConfigBuilder:
        """Enable key-based routing affinity for LWT optimization."""
        self._key_affinity = KeyRouteAffinityConfig(
            mode=mode,
            table_pk_attributes=table_pk_map or {},
        )
        return self

    def without_authentication(self) -> AlternatorConfigBuilder:
        """Disable authentication headers."""
        self._auth_enabled = False
        return self

    def with_refresh_intervals(
        self, active_ms: int = 1000, idle_ms: int = 60000
    ) -> AlternatorConfigBuilder:
        """Set node refresh intervals."""
        self._active_refresh_ms = active_ms
        self._idle_refresh_ms = idle_ms
        return self

    def with_pool_connections(self, max_connections: int) -> AlternatorConfigBuilder:
        """Set maximum pool connections per host."""
        self._max_pool_connections = max_connections
        return self

    def with_timeouts(
        self,
        discovery_seconds: float = 5.0,
        connect_seconds: float = 5.0,
        read_seconds: float = 30.0,
    ) -> AlternatorConfigBuilder:
        """Set timeout values for discovery and operations."""
        self._discovery_timeout = discovery_seconds
        self._connect_timeout = connect_seconds
        self._read_timeout = read_seconds
        return self

    def build(self) -> AlternatorConfig:
        """Build the configuration object."""
        from alternator.core.routing_scope import ClusterScope

        return AlternatorConfig(
            seed_hosts=tuple(self._seed_hosts),
            port=self._port,
            scheme=self._scheme,
            routing_scope=self._routing_scope or ClusterScope(),
            compression=self._compression,
            min_compression_size_bytes=self._min_compression_size,
            optimize_headers=self._optimize_headers,
            headers_whitelist=self._headers_whitelist,
            authentication_enabled=self._auth_enabled,
            tls=self._tls,
            key_affinity=self._key_affinity,
            max_pool_connections=self._max_pool_connections,
            active_refresh_interval_ms=self._active_refresh_ms,
            idle_refresh_interval_ms=self._idle_refresh_ms,
            discovery_timeout_seconds=self._discovery_timeout,
            connect_timeout_seconds=self._connect_timeout,
            read_timeout_seconds=self._read_timeout,
        )
