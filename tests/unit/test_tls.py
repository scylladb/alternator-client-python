"""Tests for TLS configuration utilities."""

import ssl
from pathlib import Path

import pytest

from alternator.config import TlsConfig, TlsSessionCacheConfig
from alternator.core.tls import create_ssl_context


class TestCreateSslContext:
    """Tests for create_ssl_context function."""

    def test_system_default(self) -> None:
        """Test creating SSL context with system defaults."""
        tls_config = TlsConfig.system_default()
        ctx = create_ssl_context(tls_config)

        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.check_hostname is True
        assert ctx.verify_mode == ssl.CERT_REQUIRED

    def test_trust_all_insecure(self) -> None:
        """Test creating insecure SSL context that trusts all certificates."""
        tls_config = TlsConfig.trust_all()
        ctx = create_ssl_context(tls_config)

        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.check_hostname is False
        assert ctx.verify_mode == ssl.CERT_NONE

    def test_hostname_verification_disabled(self) -> None:
        """Test disabling hostname verification."""
        tls_config = TlsConfig(verify_hostname=False)
        ctx = create_ssl_context(tls_config)

        assert ctx.check_hostname is False

    def test_hostname_verification_enabled(self) -> None:
        """Test enabling hostname verification (default)."""
        tls_config = TlsConfig(verify_hostname=True)
        ctx = create_ssl_context(tls_config)

        assert ctx.check_hostname is True

    def test_session_cache_enabled(self) -> None:
        """Test session tickets enabled when session cache is enabled."""
        tls_config = TlsConfig(session_cache=TlsSessionCacheConfig(enabled=True))
        ctx = create_ssl_context(tls_config)

        # OP_NO_TICKET should NOT be set (tickets enabled)
        assert not (ctx.options & ssl.OP_NO_TICKET)

    def test_session_cache_disabled(self) -> None:
        """Test session tickets disabled when session cache is disabled."""
        tls_config = TlsConfig(session_cache=TlsSessionCacheConfig(enabled=False))
        ctx = create_ssl_context(tls_config)

        # OP_NO_TICKET should be set (tickets disabled)
        assert ctx.options & ssl.OP_NO_TICKET

    def test_custom_ca_cert_loading(self) -> None:
        """Test loading custom CA certificate uses correct code path."""
        # Use a system CA cert that we know exists, or skip
        # This tests the code path without requiring a custom cert
        import ssl as ssl_module

        # Test with system CA files if available
        ca_file = ssl_module.get_default_verify_paths().cafile
        if ca_file and Path(ca_file).exists():
            tls_config = TlsConfig.with_custom_ca(Path(ca_file))
            ctx = create_ssl_context(tls_config)
            assert isinstance(ctx, ssl.SSLContext)
        else:
            # Skip test if no CA file available
            pytest.skip("No system CA file available for testing")

    def test_custom_ca_with_nonexistent_path(self) -> None:
        """Test error when custom CA cert path doesn't exist."""
        tls_config = TlsConfig(custom_ca_cert_paths=[Path("/nonexistent/ca.pem")])

        with pytest.raises(FileNotFoundError):
            create_ssl_context(tls_config)

    def test_multiple_custom_ca_certs(self) -> None:
        """Test loading multiple custom CA certificates uses correct code path."""
        import ssl as ssl_module

        # Get system CA paths if available
        ca_file = ssl_module.get_default_verify_paths().cafile
        if ca_file and Path(ca_file).exists():
            # Test with the same cert twice (valid scenario)
            tls_config = TlsConfig(custom_ca_cert_paths=(Path(ca_file), Path(ca_file)))
            ctx = create_ssl_context(tls_config)
            assert isinstance(ctx, ssl.SSLContext)
        else:
            pytest.skip("No system CA file available for testing")

    def test_no_system_ca_with_custom_certs(self) -> None:
        """Test disabling system CA when custom certs are provided."""
        import ssl as ssl_module

        ca_file = ssl_module.get_default_verify_paths().cafile
        if ca_file and Path(ca_file).exists():
            tls_config = TlsConfig(
                custom_ca_cert_paths=[Path(ca_file)],
                trust_system_ca_certs=False,
            )
            ctx = create_ssl_context(tls_config)
            assert ctx.verify_mode == ssl.CERT_REQUIRED
        else:
            pytest.skip("No system CA file available for testing")


class TestTlsConfig:
    """Tests for TlsConfig class."""

    def test_default_values(self) -> None:
        """Test TlsConfig default values."""
        config = TlsConfig()

        assert config.custom_ca_cert_paths == ()
        assert config.trust_system_ca_certs is True
        assert config.trust_all_certificates is False
        assert config.verify_hostname is True
        assert config.session_cache.enabled is True

    def test_trust_all_factory(self) -> None:
        """Test TlsConfig.trust_all() factory method."""
        config = TlsConfig.trust_all()

        assert config.trust_all_certificates is True
        assert config.verify_hostname is False

    def test_system_default_factory(self) -> None:
        """Test TlsConfig.system_default() factory method."""
        config = TlsConfig.system_default()

        assert config.trust_system_ca_certs is True
        assert config.trust_all_certificates is False

    def test_with_custom_ca_factory(self) -> None:
        """Test TlsConfig.with_custom_ca() factory method."""
        path1 = Path("/path/to/ca1.pem")
        path2 = Path("/path/to/ca2.pem")

        config = TlsConfig.with_custom_ca(path1, path2)

        assert config.custom_ca_cert_paths == (path1, path2)


class TestTlsSessionCacheConfig:
    """Tests for TlsSessionCacheConfig class."""

    def test_default_values(self) -> None:
        """Test TlsSessionCacheConfig default values."""
        config = TlsSessionCacheConfig()

        assert config.enabled is True
        assert config.cache_size == 1024
        assert config.timeout_seconds == 86400  # 24 hours

    def test_custom_values(self) -> None:
        """Test TlsSessionCacheConfig with custom values."""
        config = TlsSessionCacheConfig(
            enabled=False,
            cache_size=2048,
            timeout_seconds=3600,
        )

        assert config.enabled is False
        assert config.cache_size == 2048
        assert config.timeout_seconds == 3600

    def test_immutable(self) -> None:
        """Test TlsSessionCacheConfig is immutable (frozen)."""
        config = TlsSessionCacheConfig()

        with pytest.raises(AttributeError):
            config.enabled = False  # type: ignore[misc] -- testing frozen dataclass immutability
