"""Tests for configuration classes."""

from pathlib import Path

import pytest

from alternator.config import (
    AlternatorConfig,
    AlternatorConfigBuilder,
    CompressionAlgorithm,
    KeyRouteAffinityConfig,
    KeyRouteAffinityMode,
    TlsConfig,
    TlsSessionCacheConfig,
)
from alternator.core.routing_scope import ClusterScope, DatacenterScope
from alternator.exceptions import ConfigurationError


class TestAlternatorConfig:
    """Tests for AlternatorConfig validation."""

    def test_valid_config(self) -> None:
        """Test creating a valid configuration."""
        config = AlternatorConfig(
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
            AlternatorConfig(seed_hosts=[], port=8000)

    def test_missing_seeds_also_raises_value_error(self) -> None:
        """Test backward compat: ConfigurationError is also a ValueError."""
        with pytest.raises(ValueError, match="At least one seed host is required"):
            AlternatorConfig(seed_hosts=[], port=8000)

    def test_invalid_scheme_raises(self) -> None:
        """Test that invalid scheme raises ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match="scheme must be 'http' or 'https'"
        ):
            AlternatorConfig(
                seed_hosts=["localhost"],
                port=8000,
                scheme="ftp",
            )

    def test_invalid_port_zero_raises(self) -> None:
        """Test that port 0 raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="port must be 1-65535"):
            AlternatorConfig(seed_hosts=["localhost"], port=0)

    def test_invalid_port_negative_raises(self) -> None:
        """Test that negative port raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="port must be 1-65535"):
            AlternatorConfig(seed_hosts=["localhost"], port=-1)

    def test_invalid_port_too_large_raises(self) -> None:
        """Test that port > 65535 raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="port must be 1-65535"):
            AlternatorConfig(seed_hosts=["localhost"], port=65536)

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = AlternatorConfig(seed_hosts=["localhost"], port=8000)
        assert config.scheme == "http"
        assert config.request_compression.algorithm == CompressionAlgorithm.NONE
        assert config.request_compression.min_size_bytes == 1024
        assert config.header_optimization.enabled is False
        assert config.header_optimization.whitelist is None
        assert config.node_list_polling.active_interval_ms == 1000
        assert config.node_list_polling.idle_interval_ms == 60000
        assert isinstance(config.routing_scope, ClusterScope)


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
        tls = TlsConfig.trust_all()
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


class TestTlsConfig:
    """Tests for TlsConfig factory methods."""

    def test_trust_all(self) -> None:
        """Test TlsConfig.trust_all() factory."""
        tls = TlsConfig.trust_all()
        assert tls.trust_all_certificates is True
        assert tls.verify_hostname is False

    def test_system_default(self) -> None:
        """Test TlsConfig.system_default() factory."""
        tls = TlsConfig.system_default()
        assert tls.trust_system_ca_certs is True
        assert tls.trust_all_certificates is False
        assert tls.verify_hostname is True

    def test_with_custom_ca(self) -> None:
        """Test TlsConfig.with_custom_ca() factory."""
        tls = TlsConfig.with_custom_ca(
            Path("/etc/ssl/ca1.pem"),
            Path("/etc/ssl/ca2.pem"),
        )
        assert len(tls.custom_ca_cert_paths) == 2
        assert Path("/etc/ssl/ca1.pem") in tls.custom_ca_cert_paths

    def test_default_session_cache(self) -> None:
        """Test default session cache configuration."""
        tls = TlsConfig()
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
