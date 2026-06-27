"""Tests for configuration classes."""

from pathlib import Path

import pytest

from alternator.config import (
    TLS,
    AlternatorConfig,
    AlternatorConfigBuilder,
    CompressionAlgorithm,
    Config,
    HeaderWhitelistContext,
    KeyRouteAffinityConfig,
    KeyRouteAffinityMode,
    RequestCompressionConfig,
    ResponseCompression,
    RetryConfig,
    TlsConfig,
    TlsSessionCacheConfig,
)
from alternator.core.routing_scope import ClusterScope, DatacenterScope
from alternator.exceptions import ConfigurationError


class TestAlternatorConfig:
    """Tests for Config validation."""

    def test_valid_config(self) -> None:
        """Test creating a valid configuration."""
        config = Config(
            seed_hosts=["192.168.1.1", "192.168.1.2"],
            port=8000,
        )
        assert list(config.seed_hosts) == ["192.168.1.1", "192.168.1.2"]
        assert config.port == 8000
        assert config.scheme == "http"

    def test_missing_seeds_raises(self) -> None:
        """Test that empty seed_hosts raises ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match="At least one seed host is required"
        ):
            Config(seed_hosts=[], port=8000)

    def test_missing_seeds_also_raises_value_error(self) -> None:
        """Test backward compat: ConfigurationError is also a ValueError."""
        with pytest.raises(ValueError, match="At least one seed host is required"):
            Config(seed_hosts=[], port=8000)

    def test_invalid_scheme_raises(self) -> None:
        """Test that invalid scheme raises ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match="scheme must be 'http' or 'https'"
        ):
            Config(
                seed_hosts=["localhost"],
                port=8000,
                scheme="ftp",
            )

    def test_invalid_port_zero_raises(self) -> None:
        """Test that port 0 raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="port must be 1-65535"):
            Config(seed_hosts=["localhost"], port=0)

    def test_invalid_port_negative_raises(self) -> None:
        """Test that negative port raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="port must be 1-65535"):
            Config(seed_hosts=["localhost"], port=-1)

    def test_invalid_port_too_large_raises(self) -> None:
        """Test that port > 65535 raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="port must be 1-65535"):
            Config(seed_hosts=["localhost"], port=65536)

    def test_empty_aws_region_raises(self) -> None:
        """Test that empty SDK region placeholder raises."""
        with pytest.raises(ConfigurationError, match="aws_region must not be empty"):
            Config(seed_hosts=["localhost"], port=8000, aws_region="")

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = Config(seed_hosts=["localhost"], port=8000)
        assert config.scheme == "http"
        assert config.request_compression.algorithm == CompressionAlgorithm.NONE
        assert config.request_compression.min_size_bytes == 1024
        assert config.request_compression.gzip_level == 9
        assert config.response_compression == ()
        assert config.header_optimization.enabled is False
        assert config.header_optimization.whitelist is None
        assert config.header_optimization.whitelist_callback is None
        assert config.node_list_polling.active_interval_ms == 1000
        assert config.node_list_polling.idle_interval_ms == 60000
        assert config.aws_region == "us-east-1"
        assert config.user_agent.startswith("alternator-client-python/")
        assert isinstance(config.routing_scope, ClusterScope)


class TestDeprecatedConfigNames:
    """Tests for deprecated compatibility names."""

    def test_alternator_config_warns_and_builds_config(self) -> None:
        """Test deprecated AlternatorConfig compatibility name."""
        with pytest.warns(DeprecationWarning, match="AlternatorConfig"):
            config = AlternatorConfig(seed_hosts=["localhost"], port=8000)

        assert isinstance(config, Config)
        assert config.seed_hosts == ["localhost"]

    def test_tls_config_warns_and_builds_tls(self) -> None:
        """Test deprecated TlsConfig compatibility name."""
        with pytest.warns(DeprecationWarning, match="TlsConfig"):
            tls = TlsConfig()

        assert isinstance(tls, TLS)
        assert tls.trust_system_ca_certs is True

    def test_deprecated_tls_factory_warns(self) -> None:
        """Test deprecated TlsConfig factory methods remain usable."""
        with pytest.warns(DeprecationWarning, match="TlsConfig"):
            tls = TlsConfig.system_default()

        assert isinstance(tls, TLS)

    def test_top_level_preferred_exports(self) -> None:
        """Test top-level package exports preferred names."""
        import alternator

        assert alternator.Auth is not None
        assert alternator.Config is Config
        assert alternator.TLS is TLS
        assert alternator.UserAgent is not None
        assert alternator.UserAgentCustomizer is not None


class TestAlternatorConfigBuilder:
    """Tests for AlternatorConfigBuilder."""

    def test_build_minimal_config(self) -> None:
        """Test building a minimal configuration."""
        config = (
            AlternatorConfigBuilder().with_seeds("localhost").with_port(8000).build()
        )
        assert config.seed_hosts == ("localhost",)
        assert config.port == 8000
        assert config.scheme == "http"

    def test_build_with_multiple_seeds(self) -> None:
        """Test building config with multiple seed hosts."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("host1", "host2")
            .with_seeds("host3")
            .with_port(8000)
            .build()
        )
        assert config.seed_hosts == ("host1", "host2", "host3")

    def test_build_with_https(self) -> None:
        """Test building config with HTTPS."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8443)
            .with_https()
            .build()
        )
        assert config.scheme == "https"

    def test_build_with_https_and_tls_config(self) -> None:
        """Test building config with HTTPS and custom TLS."""
        tls = TLS.trust_all()
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8443)
            .with_https(tls)
            .build()
        )
        assert config.scheme == "https"
        assert config.tls.trust_all_certificates is True

    def test_build_with_datacenter(self) -> None:
        """Test building config with datacenter scope."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_datacenter("us-east-1")
            .build()
        )
        assert isinstance(config.routing_scope, DatacenterScope)
        assert config.routing_scope.datacenter == "us-east-1"
        assert config.routing_scope.fallback is None

    def test_build_with_compression(self) -> None:
        """Test building config with compression."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_compression(CompressionAlgorithm.GZIP, min_size=2048)
            .build()
        )
        assert config.request_compression.algorithm == CompressionAlgorithm.GZIP
        assert config.request_compression.min_size_bytes == 2048
        assert config.request_compression.gzip_level == 9

    def test_build_with_compression_level(self) -> None:
        """Test building config with a custom gzip compression level."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_compression(CompressionAlgorithm.GZIP, min_size=2048, gzip_level=1)
            .build()
        )
        assert config.request_compression.algorithm == CompressionAlgorithm.GZIP
        assert config.request_compression.min_size_bytes == 2048
        assert config.request_compression.gzip_level == 1

    def test_build_with_response_compression(self) -> None:
        """Test building config with response compression."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_response_compression(
                ResponseCompression.GZIP,
                ResponseCompression.DEFLATE,
            )
            .build()
        )
        assert config.response_compression == (
            ResponseCompression.GZIP,
            ResponseCompression.DEFLATE,
        )

    def test_without_response_compression(self) -> None:
        """Test disabling response compression."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_response_compression(ResponseCompression.GZIP)
            .without_response_compression()
            .build()
        )
        assert config.response_compression == ()

    def test_with_response_compression_rejects_invalid_encoding(self) -> None:
        """Test enabling response compression rejects invalid encodings."""
        with pytest.raises(
            ConfigurationError,
            match="unsupported response compression encoding",
        ):
            AlternatorConfigBuilder().with_response_compression(
                None,  # type: ignore[arg-type] -- validate runtime input
            )

    def test_invalid_response_compression_raises(self) -> None:
        """Test invalid response compression encodings raise."""
        with pytest.raises(
            ConfigurationError,
            match="unsupported response compression encoding",
        ):
            Config(
                seed_hosts=["localhost"],
                port=8000,
                response_compression=("br",),  # type: ignore[arg-type] -- validate runtime input
            )

    @pytest.mark.parametrize("gzip_level", [-1, 10])
    def test_invalid_compression_level_raises(self, gzip_level: int) -> None:
        """Test invalid gzip compression levels raise."""
        with pytest.raises(ConfigurationError, match="gzip_level must be 0-9"):
            RequestCompressionConfig(
                algorithm=CompressionAlgorithm.GZIP,
                gzip_level=gzip_level,
            )

    def test_build_with_header_optimization(self) -> None:
        """Test building config with header optimization."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_header_optimization(whitelist={"Host", "Content-Type"})
            .build()
        )
        assert config.header_optimization.enabled is True
        assert config.header_optimization.whitelist == frozenset(
            {"Host", "Content-Type"}
        )
        assert config.header_optimization.whitelist_callback is None

    def test_build_with_header_whitelist_callback(self) -> None:
        """Test building config with a dynamic header whitelist callback."""

        def whitelist_callback(context: HeaderWhitelistContext) -> set[str]:
            assert context.config.port == 8000
            return {"X-Dynamic-Header"}

        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_header_optimization(whitelist_callback=whitelist_callback)
            .build()
        )
        assert config.header_optimization.enabled is True
        assert config.header_optimization.whitelist is None
        assert config.header_optimization.whitelist_callback is whitelist_callback

    def test_build_with_key_affinity(self) -> None:
        """Test building config with key affinity."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_key_affinity(
                KeyRouteAffinityMode.RMW,
                table_pk_map={"users": "user_id"},
            )
            .build()
        )
        assert config.key_affinity.mode == KeyRouteAffinityMode.RMW
        assert config.key_affinity.table_pk_attributes == {"users": "user_id"}

    def test_build_with_refresh_intervals(self) -> None:
        """Test building config with custom refresh intervals."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_refresh_intervals(active_ms=500, idle_ms=30000)
            .build()
        )
        assert config.node_list_polling.active_interval_ms == 500
        assert config.node_list_polling.idle_interval_ms == 30000

    def test_build_with_timeouts(self) -> None:
        """Test building config with custom timeout values."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_timeouts(
                discovery_seconds=10.0,
                connect_seconds=3.0,
                read_seconds=60.0,
            )
            .build()
        )
        assert config.timeouts.discovery_seconds == 10.0
        assert config.timeouts.connect_seconds == 3.0
        assert config.timeouts.read_seconds == 60.0

    def test_build_with_timeouts_default_values(self) -> None:
        """Test that with_timeouts preserves defaults when not overridden."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_timeouts()
            .build()
        )
        assert config.timeouts.discovery_seconds == 5.0
        assert config.timeouts.connect_seconds == 5.0
        assert config.timeouts.read_seconds == 30.0

    def test_build_with_pool_connections(self) -> None:
        """Test building config with custom pool connections."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_pool_connections(500)
            .build()
        )
        assert config.max_pool_connections == 500

    def test_build_default_pool_connections(self) -> None:
        """Test default pool connections value."""
        config = (
            AlternatorConfigBuilder().with_seeds("localhost").with_port(8000).build()
        )
        assert config.max_pool_connections == 200

    def test_build_with_aws_region(self) -> None:
        """Test building config with custom SDK region placeholder."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_aws_region("us-west-2")
            .build()
        )
        assert config.aws_region == "us-west-2"

    def test_build_with_user_agent(self) -> None:
        """Test building config with user-agent callback."""

        def customize(default: str) -> str:
            return f"service-a {default}"

        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_user_agent(customize)
            .build()
        )
        assert config.user_agent is customize

    def test_build_with_user_agent_string(self) -> None:
        """Test building config with literal user-agent replacement."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_user_agent("service-a/1.0")
            .build()
        )
        assert config.user_agent == "service-a/1.0"

    def test_build_with_user_agent_none(self) -> None:
        """Test building config with explicit user-agent suppression."""
        config = (
            AlternatorConfigBuilder()
            .with_seeds("localhost")
            .with_port(8000)
            .with_user_agent(None)
            .build()
        )
        assert config.user_agent is None


class TestTlsConfig:
    """Tests for TLS factory methods."""

    def test_trust_all(self) -> None:
        """Test TLS.trust_all() factory."""
        tls = TLS.trust_all()
        assert tls.trust_all_certificates is True
        assert tls.verify_hostname is False

    def test_system_default(self) -> None:
        """Test TLS.system_default() factory."""
        tls = TLS.system_default()
        assert tls.trust_system_ca_certs is True
        assert tls.trust_all_certificates is False
        assert tls.verify_hostname is True

    def test_with_custom_ca(self) -> None:
        """Test TLS.with_custom_ca() factory."""
        tls = TLS.with_custom_ca(
            Path("/etc/ssl/ca1.pem"),
            Path("/etc/ssl/ca2.pem"),
        )
        assert len(tls.custom_ca_cert_paths) == 2
        assert Path("/etc/ssl/ca1.pem") in tls.custom_ca_cert_paths

    def test_default_session_cache(self) -> None:
        """Test default session cache configuration."""
        tls = TLS()
        assert tls.session_cache.enabled is True
        assert tls.session_cache.cache_size == 1024
        assert tls.session_cache.timeout_seconds == 86400


class TestTlsSessionCacheConfig:
    """Tests for TlsSessionCacheConfig."""

    def test_default_values(self) -> None:
        """Test default values."""
        cache = TlsSessionCacheConfig()
        assert cache.enabled is True
        assert cache.cache_size == 1024
        assert cache.timeout_seconds == 86400

    def test_custom_values(self) -> None:
        """Test custom values."""
        cache = TlsSessionCacheConfig(
            enabled=False,
            cache_size=512,
            timeout_seconds=3600,
        )
        assert cache.enabled is False
        assert cache.cache_size == 512
        assert cache.timeout_seconds == 3600


class TestKeyRouteAffinityConfig:
    """Tests for KeyRouteAffinityConfig."""

    def test_default_mode_is_none(self) -> None:
        """Test default mode is NONE."""
        config = KeyRouteAffinityConfig()
        assert config.mode == KeyRouteAffinityMode.NONE

    def test_with_rmw_mode(self) -> None:
        """Test RMW mode configuration."""
        config = KeyRouteAffinityConfig(
            mode=KeyRouteAffinityMode.RMW,
            table_pk_attributes={"users": "pk"},
        )
        assert config.mode == KeyRouteAffinityMode.RMW
        assert config.table_pk_attributes == {"users": "pk"}


class TestRetryConfig:
    """Tests for RetryConfig validation."""

    def test_zero_max_attempts_raises(self) -> None:
        """Retry attempts count is a total-attempts value and must be positive."""
        with pytest.raises(ConfigurationError, match="max_attempts must be > 0"):
            RetryConfig(max_attempts=0)
