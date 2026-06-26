"""Tests for TLS configuration utilities."""

import ssl
from pathlib import Path
from unittest.mock import patch

import pytest

from alternator.config import TLS, TlsSessionCacheConfig
from alternator.core.tls import create_ssl_context
from alternator.exceptions import ConfigurationError


class TestCreateSslContext:
    """Tests for create_ssl_context function."""

    def test_system_default(self) -> None:
        """Test creating SSL context with system defaults."""
        tls_config = TLS.system_default()
        ctx = create_ssl_context(tls_config)

        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.check_hostname is True
        assert ctx.verify_mode == ssl.CERT_REQUIRED

    def test_trust_all_insecure(self) -> None:
        """Test creating insecure SSL context that trusts all certificates."""
        tls_config = TLS.trust_all()
        ctx = create_ssl_context(tls_config)

        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.check_hostname is False
        assert ctx.verify_mode == ssl.CERT_NONE

    def test_hostname_verification_disabled(self) -> None:
        """Test disabling hostname verification."""
        tls_config = TLS(verify_hostname=False)
        ctx = create_ssl_context(tls_config)

        assert ctx.check_hostname is False

    def test_hostname_verification_enabled(self) -> None:
        """Test enabling hostname verification (default)."""
        tls_config = TLS(verify_hostname=True)
        ctx = create_ssl_context(tls_config)

        assert ctx.check_hostname is True

    def test_session_cache_enabled(self) -> None:
        """Test session tickets enabled when session cache is enabled."""
        tls_config = TLS(session_cache=TlsSessionCacheConfig(enabled=True))
        ctx = create_ssl_context(tls_config)

        # OP_NO_TICKET should NOT be set (tickets enabled)
        assert not (ctx.options & ssl.OP_NO_TICKET)

    def test_session_cache_disabled(self) -> None:
        """Test session tickets disabled when session cache is disabled."""
        tls_config = TLS(session_cache=TlsSessionCacheConfig(enabled=False))
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
            tls_config = TLS.with_custom_ca(Path(ca_file))
            ctx = create_ssl_context(tls_config)
            assert isinstance(ctx, ssl.SSLContext)
        else:
            # Skip test if no CA file available
            pytest.skip("No system CA file available for testing")

    def test_custom_ca_with_nonexistent_path(self) -> None:
        """Test error when custom CA cert path doesn't exist."""
        tls_config = TLS(custom_ca_cert_paths=[Path("/nonexistent/ca.pem")])

        with pytest.raises(FileNotFoundError):
            create_ssl_context(tls_config)

    def test_multiple_custom_ca_certs(self) -> None:
        """Test loading multiple custom CA certificates uses correct code path."""
        import ssl as ssl_module

        # Get system CA paths if available
        ca_file = ssl_module.get_default_verify_paths().cafile
        if ca_file and Path(ca_file).exists():
            # Test with the same cert twice (valid scenario)
            tls_config = TLS(custom_ca_cert_paths=(Path(ca_file), Path(ca_file)))
            ctx = create_ssl_context(tls_config)
            assert isinstance(ctx, ssl.SSLContext)
        else:
            pytest.skip("No system CA file available for testing")

    def test_no_system_ca_with_custom_certs(self) -> None:
        """Test disabling system CA when custom certs are provided."""
        import ssl as ssl_module

        ca_file = ssl_module.get_default_verify_paths().cafile
        if ca_file and Path(ca_file).exists():
            tls_config = TLS(
                custom_ca_cert_paths=[Path(ca_file)],
                trust_system_ca_certs=False,
            )
            ctx = create_ssl_context(tls_config)
            assert ctx.verify_mode == ssl.CERT_REQUIRED
        else:
            pytest.skip("No system CA file available for testing")

    def test_client_cert_chain_is_loaded(self, tmp_path: Path) -> None:
        """Test TLS client certificate paths load into the SSL context."""
        cert_path = tmp_path / "client.crt"
        key_path = tmp_path / "client.key"

        with patch("ssl.SSLContext.load_cert_chain") as mock_load:
            tls_config = TLS(
                client_cert_path=cert_path,
                client_key_path=key_path,
            )
            create_ssl_context(tls_config)

        mock_load.assert_called_once_with(
            certfile=str(cert_path),
            keyfile=str(key_path),
        )

    def test_combined_client_cert_chain_is_loaded(self, tmp_path: Path) -> None:
        """Test combined certificate/key PEM path loads without a key path."""
        cert_path = tmp_path / "client-combined.pem"

        with patch("ssl.SSLContext.load_cert_chain") as mock_load:
            tls_config = TLS(client_cert_path=cert_path)
            create_ssl_context(tls_config)

        mock_load.assert_called_once_with(
            certfile=str(cert_path),
            keyfile=None,
        )

    def test_key_log_file_is_configured(self, tmp_path: Path) -> None:
        """Test TLS key log path is assigned to the SSL context when available."""
        key_log_path = tmp_path / "tls.keys"
        tls_config = TLS(key_log_file_path=key_log_path)
        ctx = create_ssl_context(tls_config)

        if hasattr(ctx, "keylog_filename"):
            assert ctx.keylog_filename == str(key_log_path)
            assert key_log_path.parent == tmp_path


class TestTlsConfig:
    """Tests for TLS class."""

    def test_default_values(self) -> None:
        """Test TLS default values."""
        config = TLS()

        assert config.custom_ca_cert_paths == ()
        assert config.trust_system_ca_certs is True
        assert config.trust_all_certificates is False
        assert config.verify_hostname is True
        assert config.session_cache.enabled is True
        assert config.client_cert_path is None
        assert config.client_key_path is None
        assert config.key_log_file_path is None

    def test_trust_all_factory(self) -> None:
        """Test TLS.trust_all() factory method."""
        config = TLS.trust_all()

        assert config.trust_all_certificates is True
        assert config.verify_hostname is False

    def test_system_default_factory(self) -> None:
        """Test TLS.system_default() factory method."""
        config = TLS.system_default()

        assert config.trust_system_ca_certs is True
        assert config.trust_all_certificates is False

    def test_with_custom_ca_factory(self) -> None:
        """Test TLS.with_custom_ca() factory method."""
        path1 = Path("/path/to/ca1.pem")
        path2 = Path("/path/to/ca2.pem")

        config = TLS.with_custom_ca(path1, path2)

        assert config.custom_ca_cert_paths == (path1, path2)

    def test_client_key_without_cert_raises(self) -> None:
        """Test client private key path requires a client certificate path."""
        with pytest.raises(ConfigurationError, match="client_key_path requires"):
            TLS(client_key_path=Path("/path/to/client.key"))

    def test_sdk_client_cert_with_cert_and_key(self) -> None:
        """Test botocore client_cert value for separate cert and key files."""
        config = TLS(
            client_cert_path=Path("/path/to/client.crt"),
            client_key_path=Path("/path/to/client.key"),
        )

        assert config.sdk_client_cert == (
            "/path/to/client.crt",
            "/path/to/client.key",
        )

    def test_sdk_client_cert_with_combined_cert(self) -> None:
        """Test botocore client_cert value for a combined cert/key file."""
        config = TLS(client_cert_path=Path("/path/to/client-combined.pem"))

        assert config.sdk_client_cert == "/path/to/client-combined.pem"


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
