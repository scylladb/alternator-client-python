"""Configuration classes for Alternator load balancing client."""

from __future__ import annotations

import logging
import os
import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Literal

from alternator._version import __version__
from alternator.core.routing_scope import ClusterScope, RoutingScope
from alternator.exceptions import ConfigurationError

logger = logging.getLogger("alternator")

SDKClientCert = str | tuple[str, str]
UserAgentCustomizer = Callable[[str], str]
UserAgent = str | UserAgentCustomizer
DEFAULT_USER_AGENT = f"alternator-client-python/{__version__}"


class CompressionAlgorithm(Enum):
    """Compression algorithm for request bodies."""

    NONE = auto()
    GZIP = auto()


class ResponseCompression(Enum):
    """HTTP response compression encoding accepted by the client."""

    GZIP = "gzip"
    DEFLATE = "deflate"


class KeyRouteAffinityMode(Enum):
    """Mode for key-based routing affinity (LWT optimization)."""

    NONE = auto()  # Default round-robin
    RMW = auto()  # Read-modify-write operations only
    ANY_WRITE = auto()  # All write operations


@dataclass(frozen=True)
class TLS:
    """TLS/SSL configuration for HTTPS connections."""

    # Certificate trust settings
    custom_ca_cert_paths: Sequence[Path] = field(default_factory=tuple)
    trust_system_ca_certs: bool = True
    trust_all_certificates: bool = False  # INSECURE - dev only

    # Verification settings
    verify_hostname: bool = True

    # TLS session tickets
    session_tickets_enabled: bool = True

    # Client certificate authentication
    client_cert_path: Path | None = None
    client_key_path: Path | None = None

    # TLS key logging for traffic debugging
    key_log_file_path: Path | None = None

    def __post_init__(self) -> None:
        """Validate TLS configuration values."""
        if self.client_key_path is not None and self.client_cert_path is None:
            raise ConfigurationError("client_key_path requires client_cert_path")

    @property
    def sdk_client_cert(self) -> SDKClientCert | None:
        """Return the botocore client_cert value for this TLS config."""
        if self.client_cert_path is None:
            return None
        if self.client_key_path is not None:
            return (str(self.client_cert_path), str(self.client_key_path))
        return str(self.client_cert_path)

    @classmethod
    def trust_all(cls) -> TLS:
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
            "TLS.trust_all() disables TLS certificate verification. "
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
    def system_default(cls) -> TLS:
        """Create config using system CA certificates."""
        return cls(trust_system_ca_certs=True)

    @classmethod
    def with_custom_ca(cls, *cert_paths: Path) -> TLS:
        """Create config with custom CA certificates."""
        return cls(custom_ca_cert_paths=tuple(cert_paths))


@dataclass(frozen=True)
class RequestCompressionConfig:
    """Configuration for request compression."""

    algorithm: CompressionAlgorithm = CompressionAlgorithm.NONE
    min_size_bytes: int = 1024
    gzip_level: int = 9

    def __post_init__(self) -> None:
        if self.min_size_bytes < 0:
            raise ConfigurationError(
                f"min_size_bytes must be >= 0, got {self.min_size_bytes}"
            )
        if self.gzip_level < 0 or self.gzip_level > 9:
            raise ConfigurationError(f"gzip_level must be 0-9, got {self.gzip_level}")

    @property
    def enabled(self) -> bool:
        """Whether compression is active."""
        return self.algorithm != CompressionAlgorithm.NONE


@dataclass(frozen=True)
class HeaderWhitelistContext:
    """Context passed to custom header whitelist callbacks."""

    config: Config
    auth_enabled: bool
    compression_enabled: bool
    required_headers: frozenset[str]


HeaderWhitelistCallback = Callable[
    [HeaderWhitelistContext],
    frozenset[str] | set[str],
]


@dataclass(frozen=True)
class HeaderOptimizationConfig:
    """Configuration for request header optimization.

    When enabled, non-essential HTTP headers are stripped from requests
    to reduce bandwidth. Authentication headers (Authorization,
    X-Amz-Date, X-Amz-Security-Token) are preserved when explicit
    static credentials are passed with ``auth=Auth.static_credentials(...)``.
    """

    enabled: bool = False
    whitelist: frozenset[str] | None = None
    whitelist_callback: HeaderWhitelistCallback | None = None


@dataclass(frozen=True)
class Auth:
    """
    Explicit Alternator authentication settings.

    Alternator client authentication currently supports only static
    credentials. Environment, profile, and provider-chain credentials are not
    supported by this API.
    """

    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None

    def __post_init__(self) -> None:
        has_key = self.access_key_id is not None
        has_secret = self.secret_access_key is not None
        if has_key != has_secret:
            raise ConfigurationError(
                "Static auth requires both access_key_id and secret_access_key"
            )
        if self.session_token is not None and not has_key:
            raise ConfigurationError(
                "session_token requires static access_key_id and secret_access_key"
            )
        if self.access_key_id == "" or self.secret_access_key == "":
            raise ConfigurationError("Static auth credentials must not be empty")

    @classmethod
    def disabled(cls) -> Auth:
        """Create auth settings with request signing disabled."""
        return cls()

    @classmethod
    def static_credentials(
        cls,
        access_key_id: str,
        secret_access_key: str,
        session_token: str | None = None,
    ) -> Auth:
        """Create auth settings using static Alternator credentials."""
        return cls(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
        )

    @property
    def enabled(self) -> bool:
        """Whether request signing is enabled."""
        return self.access_key_id is not None

    def as_boto_kwargs(self) -> dict[str, str]:
        """Return boto credential kwargs for static auth."""
        if not self.enabled:
            return {}

        if self.access_key_id is None or self.secret_access_key is None:
            raise ConfigurationError(
                "Static auth requires both access_key_id and secret_access_key"
            )

        kwargs = {
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
        }
        if self.session_token is not None:
            kwargs["aws_session_token"] = self.session_token
        return kwargs


class RetryMode(Enum):
    """Retry mode for boto3/botocore requests."""

    LEGACY = "legacy"
    STANDARD = "standard"
    ADAPTIVE = "adaptive"


@dataclass(frozen=True)
class RetryConfig:
    """Retry settings for DynamoDB operations."""

    max_attempts: int = 3
    mode: RetryMode = RetryMode.STANDARD

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise ConfigurationError(
                f"max_attempts must be > 0, got {self.max_attempts}"
            )


@dataclass(frozen=True)
class TimeoutConfig:
    """Timeout settings for Alternator operations (in seconds)."""

    discovery_seconds: float = 5.0  # Timeout for /localnodes discovery requests
    connect_seconds: float = 5.0  # TCP connection timeout for DynamoDB operations
    read_seconds: float = 30.0  # Read timeout for DynamoDB operations

    def __post_init__(self) -> None:
        for field_name in ("discovery_seconds", "connect_seconds", "read_seconds"):
            value = getattr(self, field_name)
            if value <= 0:
                raise ConfigurationError(f"{field_name} must be > 0, got {value}")


@dataclass(frozen=True)
class NodeListPollingConfig:
    """Configuration for node list refresh intervals."""

    active_interval_ms: int = 1000  # Refresh interval when client is active (1 second)
    idle_interval_ms: int = 60000  # Refresh interval when client is idle (1 minute)

    def __post_init__(self) -> None:
        if self.active_interval_ms <= 0:
            raise ConfigurationError(
                f"active_interval_ms must be > 0, got {self.active_interval_ms}"
            )
        if self.idle_interval_ms <= 0:
            raise ConfigurationError(
                f"idle_interval_ms must be > 0, got {self.idle_interval_ms}"
            )


@dataclass(frozen=True)
class KeyRouteAffinityConfig:
    """Configuration for LWT-optimized routing."""

    mode: KeyRouteAffinityMode = KeyRouteAffinityMode.NONE

    # Table name -> partition key attribute name mapping
    # If not provided, will auto-discover via DescribeTable
    table_pk_attributes: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Config:
    """
    Main configuration for Alternator load balancing client.

    Example:
        config = Config(
            seed_hosts=["192.168.1.1", "192.168.1.2"],
            port=8000,
        )
    """

    # Required: Initial nodes for discovery
    seed_hosts: Sequence[str]

    # Required: Connection settings
    port: int
    scheme: Literal["http", "https"] = "http"

    # Routing scope for topology awareness
    routing_scope: RoutingScope = field(default_factory=ClusterScope)

    # Request compression settings
    request_compression: RequestCompressionConfig = field(
        default_factory=RequestCompressionConfig
    )

    # Response compression settings
    response_compression: Sequence[ResponseCompression] = field(default_factory=tuple)

    # Header optimization
    header_optimization: HeaderOptimizationConfig = field(
        default_factory=HeaderOptimizationConfig
    )

    # TLS configuration (used when scheme="https")
    tls: TLS = field(default_factory=TLS.system_default)

    # Key route affinity for LWT optimization
    key_affinity: KeyRouteAffinityConfig = field(default_factory=KeyRouteAffinityConfig)

    # Retry settings
    retries: RetryConfig = field(default_factory=RetryConfig)

    # Connection pooling
    max_pool_connections: int = 200  # Max connections per host

    # Node list refresh intervals
    node_list_polling: NodeListPollingConfig = field(
        default_factory=NodeListPollingConfig
    )

    # Timeout settings
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)

    # SDK settings
    aws_region: str = "us-east-1"
    user_agent: UserAgent | None = DEFAULT_USER_AGENT

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
        if self.max_pool_connections <= 0:
            raise ConfigurationError(
                f"max_pool_connections must be > 0, got {self.max_pool_connections}"
            )
        if not self.aws_region:
            raise ConfigurationError("aws_region must not be empty")
        object.__setattr__(
            self,
            "response_compression",
            _normalize_response_compression(self.response_compression),
        )


def build_sdk_config_kwargs(config: Config) -> dict[str, Any]:
    """Build SDK config kwargs shared by sync and async clients."""
    kwargs: dict[str, Any] = {
        "retries": {
            "total_max_attempts": config.retries.max_attempts,
            "mode": config.retries.mode.value,
        },
        "max_pool_connections": config.max_pool_connections,
        "connect_timeout": config.timeouts.connect_seconds,
        "read_timeout": config.timeouts.read_seconds,
    }
    if config.scheme == "https" and config.tls.sdk_client_cert is not None:
        kwargs["client_cert"] = config.tls.sdk_client_cert
    if config.user_agent is not None:
        kwargs["user_agent"] = _resolve_user_agent(config.user_agent)
    else:
        kwargs.pop("user_agent", None)
    return kwargs


def _resolve_user_agent(user_agent: UserAgent) -> str:
    """Return the User-Agent produced from the default Alternator identity."""
    resolved = (
        user_agent if isinstance(user_agent, str) else user_agent(DEFAULT_USER_AGENT)
    )
    if not isinstance(resolved, str) or not resolved:
        raise ConfigurationError(
            "user_agent must be a non-empty string or callback returning one"
        )
    return resolved


def _normalize_response_compression(
    encodings: Sequence[ResponseCompression],
) -> tuple[ResponseCompression, ...]:
    """Validate and freeze response compression encodings."""
    normalized = tuple(encodings)
    for encoding in normalized:
        if not isinstance(encoding, ResponseCompression):
            raise ConfigurationError(
                f"unsupported response compression encoding: {encoding!r}"
            )
    return normalized
