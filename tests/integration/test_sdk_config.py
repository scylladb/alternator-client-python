"""Integration tests for SDK client configuration propagation."""

from __future__ import annotations

import pytest

from alternator import (
    Config,
    RetryConfig,
    RetryMode,
    TimeoutConfig,
    close_client,
    create_client,
)
from alternator.async_client import close_async_client, create_async_client
from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


def _transport_config(user_agent_label: str) -> Config:
    return Config(
        seed_hosts=[SCYLLA_HOST],
        port=SCYLLA_PORT,
        scheme="http",
        retries=RetryConfig(max_attempts=4, mode=RetryMode.STANDARD),
        max_pool_connections=37,
        timeouts=TimeoutConfig(
            discovery_seconds=2.0,
            connect_seconds=3.0,
            read_seconds=11.0,
        ),
        aws_region="us-west-2",
        user_agent=user_agent_label,
    )


class TestSdkConfigPropagation:
    """Verify transport, retry, and SDK options reach the real sync client."""

    def test_client_applies_sdk_config_and_still_operates(self) -> None:
        config = _transport_config("alternator-sdk-config-sync")
        client = create_client(config)
        try:
            sdk_config = client.meta.config
            assert client.meta.region_name == "us-west-2"
            assert sdk_config.max_pool_connections == 37
            assert sdk_config.connect_timeout == 3.0
            assert sdk_config.read_timeout == 11.0
            assert sdk_config.retries["mode"] == RetryMode.STANDARD.value
            assert sdk_config.user_agent.startswith("alternator-sdk-config-sync")
            assert "TableNames" in client.list_tables()
        finally:
            close_client(client)


class TestAsyncSdkConfigPropagation:
    """Verify transport, retry, and SDK options reach the real async client."""

    @pytest.mark.asyncio
    async def test_client_applies_sdk_config_and_still_operates(self) -> None:
        config = _transport_config("alternator-sdk-config-async")
        client = await create_async_client(config)
        try:
            sdk_config = client.meta.config
            assert client.meta.region_name == "us-west-2"
            assert sdk_config.max_pool_connections == 37
            assert sdk_config.connect_timeout == 3.0
            assert sdk_config.read_timeout == 11.0
            assert sdk_config.retries["mode"] == RetryMode.STANDARD.value
            assert sdk_config.user_agent.startswith("alternator-sdk-config-async")
            assert "TableNames" in await client.list_tables()
        finally:
            await close_async_client(client)
