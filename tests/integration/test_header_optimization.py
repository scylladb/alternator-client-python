"""Integration tests for prepared-request header filtering."""

from __future__ import annotations

import pytest

from alternator import AlternatorConfigBuilder, Auth
from alternator import client as alternator_client
from alternator.async_client import AsyncSession
from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION
from tests.integration.wire_capture import (
    capture_prepared_requests,
    inject_custom_headers,
    latest_request,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


def _user_agent_config(user_agent_label: str) -> object:
    return (
        AlternatorConfigBuilder()
        .with_seeds(SCYLLA_HOST)
        .with_port(SCYLLA_PORT)
        .with_user_agent(user_agent_label)
        .build()
    )


class TestHeaderOptimization:
    """Verify sync header filtering on prepared requests."""

    def test_disabled_keeps_user_agent_on_wire(self) -> None:
        with alternator_client(
            "dynamodb",
            cluster_config=_user_agent_config("alternator-unfiltered"),
        ) as client:
            captured = capture_prepared_requests(client)
            client.list_tables()
            request = latest_request(captured)
            assert request.header("user-agent") == "alternator-unfiltered"

    def test_filters_wire_headers_and_preserves_auth(self) -> None:
        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .with_header_optimization(whitelist={"X-Keep-Me"})
            .with_user_agent("alternator-filtered")
            .build()
        )
        with alternator_client(
            "dynamodb",
            cluster_config=config,
            auth=Auth.static_credentials("alternator", "secret"),
        ) as client:
            inject_custom_headers(client)
            captured = capture_prepared_requests(client)
            client.list_tables()
            request = latest_request(captured)

            assert request.header("x-keep-me") == "keep"
            assert request.header("x-drop-me") is None
            assert request.header("authorization") is not None
            assert request.header("x-amz-date") is not None
            assert request.header("user-agent") == "alternator-filtered"


class TestAsyncHeaderOptimization:
    """Verify async header filtering on prepared requests."""

    @pytest.mark.asyncio
    async def test_filters_wire_headers_and_preserves_auth(self) -> None:
        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .with_header_optimization(whitelist={"X-Keep-Me"})
            .build()
        )
        async with AsyncSession(
            config,
            auth=Auth.static_credentials("alternator", "secret"),
        ) as session:
            client = await session.client("dynamodb")
            inject_custom_headers(client)
            captured = capture_prepared_requests(client)
            await client.list_tables()
            request = latest_request(captured)

            assert request.header("x-keep-me") == "keep"
            assert request.header("x-drop-me") is None
            assert request.header("authorization") is not None
            assert request.header("x-amz-date") is not None
