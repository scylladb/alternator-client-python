"""Tests for internal BotoConfig creation."""

import contextlib
from pathlib import Path
from typing import Any, cast

import pytest
from botocore import UNSIGNED

from alternator.client import _create_boto_config
from alternator.config import (
    _DEFAULT_USER_AGENT,
    TLS,
    Config,
    RetryConfig,
    RetryMode,
    TimeoutConfig,
)


class TestCreateBotoConfig:
    """Tests for _create_boto_config."""

    def _make_config(self, **overrides: object) -> Config:
        defaults = {
            "seed_hosts": ["localhost"],
            "port": 9998,
        }
        defaults.update(overrides)
        return Config(**cast(Any, defaults))

    def test_unsigned_when_no_credentials(self) -> None:
        """Without credentials the signature must be UNSIGNED."""
        config = self._make_config()
        boto_config: Any = _create_boto_config(config, auth_enabled=False)

        # BotoConfig stores user-provided options in _user_provided_options
        assert boto_config.signature_version is UNSIGNED

    def test_no_unsigned_when_credentials_provided(self) -> None:
        """With credentials the signature_version must not be set."""
        config = self._make_config()
        boto_config: Any = _create_boto_config(config, auth_enabled=True)

        assert "signature_version" not in boto_config._user_provided_options

    def test_retry_settings_propagated(self) -> None:
        """Retry config from Config flows into BotoConfig."""
        config = self._make_config(
            retries=RetryConfig(max_attempts=5, mode=RetryMode.ADAPTIVE),
        )
        boto_config: Any = _create_boto_config(config, auth_enabled=False)

        retries = boto_config._user_provided_options["retries"]
        assert retries["total_max_attempts"] == 5
        assert retries["mode"] == "adaptive"

    def test_retry_attempts_are_total_attempts_in_created_client(self) -> None:
        """RetryConfig max_attempts maps to botocore total attempts."""
        import boto3

        config = self._make_config(retries=RetryConfig(max_attempts=3))
        boto_config: Any = _create_boto_config(config, auth_enabled=False)
        client = boto3.client(
            "dynamodb",
            endpoint_url="http://localhost:1",
            config=boto_config,
            region_name="us-east-1",
        )

        assert cast(Any, client.meta.config).retries["total_max_attempts"] == 3

    def test_pool_connections_propagated(self) -> None:
        """max_pool_connections from Config flows into BotoConfig."""
        config = self._make_config(max_pool_connections=42)
        boto_config: Any = _create_boto_config(config, auth_enabled=False)

        assert boto_config.max_pool_connections == 42

    def test_timeouts_propagated(self) -> None:
        """Connect/read timeout settings flow into BotoConfig."""
        config = self._make_config(
            timeouts=TimeoutConfig(
                discovery_seconds=1.0,
                connect_seconds=2.5,
                read_seconds=7.5,
            ),
        )
        boto_config: Any = _create_boto_config(config, auth_enabled=False)

        assert boto_config.connect_timeout == 2.5
        assert boto_config.read_timeout == 7.5

    def test_default_user_agent_propagated(self) -> None:
        """By default Alternator supplies its own SDK user-agent."""
        config = self._make_config()
        boto_config: Any = _create_boto_config(config, auth_enabled=False)

        assert boto_config.user_agent == _DEFAULT_USER_AGENT
        assert boto_config._user_provided_options["user_agent"] == _DEFAULT_USER_AGENT

    def test_user_agent_callback_can_append_to_current_value(self) -> None:
        """Callback can keep the SDK user-agent and add Alternator identity."""
        current = "Boto3/1.0 Botocore/1.0"

        config = self._make_config(user_agent=lambda default: f"{current} {default}")
        boto_config: Any = _create_boto_config(config, auth_enabled=False)

        assert boto_config.user_agent == f"Boto3/1.0 Botocore/1.0 {_DEFAULT_USER_AGENT}"

    def test_user_agent_callback_can_wrap_default_value(self) -> None:
        """Callback can place the Alternator identity inside another string."""
        config = self._make_config(user_agent=lambda default: f"service-a ({default})")
        boto_config: Any = _create_boto_config(config, auth_enabled=False)

        assert boto_config.user_agent == f"service-a ({_DEFAULT_USER_AGENT})"

    def test_user_agent_callback_can_replace_default_value(self) -> None:
        """Callback can replace the Alternator identity completely."""
        config = self._make_config(user_agent=lambda _default: "orders-service/1.0")
        boto_config: Any = _create_boto_config(config, auth_enabled=False)

        assert boto_config.user_agent == "orders-service/1.0"

    def test_user_agent_string_replaces_default_value(self) -> None:
        """A literal user-agent value replaces the Alternator identity."""
        config = self._make_config(user_agent="orders-service/1.0")
        boto_config: Any = _create_boto_config(config, auth_enabled=False)

        assert boto_config.user_agent == "orders-service/1.0"

    def test_user_agent_callback_must_return_non_empty_string(self) -> None:
        """Invalid user-agent callback results fail during SDK config creation."""
        config = self._make_config(user_agent=lambda _default: "")

        with pytest.raises(ValueError, match="user_agent"):
            _create_boto_config(config, auth_enabled=False)

    def test_user_agent_string_must_be_non_empty(self) -> None:
        """Invalid literal user-agent values fail during SDK config creation."""
        config = self._make_config(user_agent="")

        with pytest.raises(ValueError, match="user_agent"):
            _create_boto_config(config, auth_enabled=False)

    def test_client_cert_propagated_for_https(self) -> None:
        """TLS client certificate settings flow into BotoConfig."""
        config = self._make_config(
            scheme="https",
            tls=TLS(
                client_cert_path=Path("/path/to/client.crt"),
                client_key_path=Path("/path/to/client.key"),
            ),
        )
        boto_config: Any = _create_boto_config(config, auth_enabled=False)

        assert boto_config.client_cert == (
            "/path/to/client.crt",
            "/path/to/client.key",
        )

    def test_combined_client_cert_propagated_for_https(self) -> None:
        """A combined client certificate/key file flows into BotoConfig."""
        config = self._make_config(
            scheme="https",
            tls=TLS(client_cert_path=Path("/path/to/client-combined.pem")),
        )
        boto_config: Any = _create_boto_config(config, auth_enabled=False)

        assert boto_config.client_cert == "/path/to/client-combined.pem"


class TestCreateAioConfig:
    """Tests for async SDK config creation."""

    def test_async_config_matches_sync_transport_settings(self) -> None:
        """AioConfig receives retry, timeout, pool, and signature settings."""
        pytest.importorskip("aiobotocore")

        from alternator.async_client import _create_aio_config

        config = Config(
            seed_hosts=["localhost"],
            port=9998,
            scheme="https",
            tls=TLS(
                client_cert_path=Path("/path/to/client.crt"),
                client_key_path=Path("/path/to/client.key"),
            ),
            retries=RetryConfig(max_attempts=4, mode=RetryMode.STANDARD),
            max_pool_connections=17,
            timeouts=TimeoutConfig(
                discovery_seconds=1.0,
                connect_seconds=3.0,
                read_seconds=11.0,
            ),
        )
        aio_config: Any = _create_aio_config(config, auth_enabled=False)

        retries = aio_config._user_provided_options["retries"]
        assert retries["total_max_attempts"] == 4
        assert retries["mode"] == "standard"
        assert aio_config.max_pool_connections == 17
        assert aio_config.connect_timeout == 3.0
        assert aio_config.read_timeout == 11.0
        assert aio_config.signature_version is UNSIGNED
        assert aio_config.client_cert == (
            "/path/to/client.crt",
            "/path/to/client.key",
        )
        assert aio_config.user_agent == _DEFAULT_USER_AGENT
        assert aio_config._user_provided_options["user_agent"] == _DEFAULT_USER_AGENT


class TestUnsignedRequestNoAuthHeader:
    """Verify that UNSIGNED config produces requests without auth headers."""

    def test_no_authorization_header_on_request(self) -> None:
        """A boto3 client with UNSIGNED signature must not add Authorization."""
        import boto3
        from botocore.awsrequest import AWSPreparedRequest

        config = self._make_config()
        boto_cfg = _create_boto_config(config, auth_enabled=False)

        client = boto3.client(
            "dynamodb",
            endpoint_url="http://localhost:1",
            config=boto_cfg,
            region_name="us-east-1",
        )

        # Capture the prepared request via the before-send event
        captured: list[AWSPreparedRequest] = []

        def capture_request(request: AWSPreparedRequest, **_: object) -> None:
            captured.append(request)
            # Raise to prevent actual network call
            raise ConnectionError("intercepted")

        client.meta.events.register("before-send.dynamodb.ListTables", capture_request)

        with contextlib.suppress(Exception):
            client.list_tables()

        assert len(captured) == 1
        assert "Authorization" not in captured[0].headers

    @staticmethod
    def _make_config(**overrides: object) -> Config:
        defaults = {
            "seed_hosts": ["localhost"],
            "port": 9998,
        }
        defaults.update(overrides)
        return Config(**cast(Any, defaults))
