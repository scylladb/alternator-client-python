"""Tests for internal BotoConfig creation."""

import contextlib

from botocore import UNSIGNED

from alternator.client import _create_boto_config
from alternator.config import Config, RetryConfig, RetryMode


class TestCreateBotoConfig:
    """Tests for _create_boto_config."""

    def _make_config(self, **overrides: object) -> Config:
        defaults = {
            "seed_hosts": ["localhost"],
            "port": 9998,
        }
        defaults.update(overrides)
        return Config(**defaults)  # type: ignore[arg-type]  -- test helper

    def test_unsigned_when_no_credentials(self) -> None:
        """Without credentials the signature must be UNSIGNED."""
        config = self._make_config()
        boto_config = _create_boto_config(config, auth_enabled=False)

        # BotoConfig stores user-provided options in _user_provided_options
        assert boto_config.signature_version is UNSIGNED

    def test_no_unsigned_when_credentials_provided(self) -> None:
        """With credentials the signature_version must not be set."""
        config = self._make_config()
        boto_config = _create_boto_config(config, auth_enabled=True)

        assert "signature_version" not in boto_config._user_provided_options

    def test_retry_settings_propagated(self) -> None:
        """Retry config from Config flows into BotoConfig."""
        config = self._make_config(
            retries=RetryConfig(max_attempts=5, mode=RetryMode.ADAPTIVE),
        )
        boto_config = _create_boto_config(config, auth_enabled=False)

        retries = boto_config._user_provided_options["retries"]
        assert retries["max_attempts"] == 5
        assert retries["mode"] == "adaptive"

    def test_pool_connections_propagated(self) -> None:
        """max_pool_connections from Config flows into BotoConfig."""
        config = self._make_config(max_pool_connections=42)
        boto_config = _create_boto_config(config, auth_enabled=False)

        assert boto_config.max_pool_connections == 42


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
        return Config(**defaults)  # type: ignore[arg-type]  -- test helper
