"""Integration tests for prepared-request header filtering."""

from __future__ import annotations

from typing import Any

import pytest

from alternator import AlternatorConfigBuilder, Auth, close_client, create_client
from alternator.async_client import close_async_client, create_async_client
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
    def customize_sdk(kwargs: dict[str, Any]) -> None:
        kwargs["user_agent_extra"] = user_agent_label

    return (
        AlternatorConfigBuilder()
        .with_seeds(SCYLLA_HOST)
        .with_port(SCYLLA_PORT)
        .with_sdk_config_customizer(customize_sdk)
        .build()
    )


class TestHeaderOptimization:
    """Verify sync header filtering on prepared requests."""

    def test_disabled_keeps_user_agent_on_wire(self) -> None:
        client = create_client(_user_agent_config("alternator-unfiltered"))
        try:
            captured = capture_prepared_requests(client)
            client.list_tables()
            request = latest_request(captured)
            assert "alternator-unfiltered" in (request.header("user-agent") or "")
        finally:
            close_client(client)

    def test_filters_wire_headers_and_preserves_auth(self) -> None:
        def customize_sdk(kwargs: dict[str, Any]) -> None:
            kwargs["user_agent_extra"] = "alternator-filtered"

        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .with_header_optimization(whitelist={"X-Keep-Me"})
            .with_sdk_config_customizer(customize_sdk)
            .build()
        )
        client = create_client(
            config,
            auth=Auth.static_credentials("alternator", "secret"),
        )
        try:
            inject_custom_headers(client)
            captured = capture_prepared_requests(client)
            client.list_tables()
            request = latest_request(captured)

            assert request.header("x-keep-me") == "keep"
            assert request.header("x-drop-me") is None
            assert request.header("authorization") is not None
            assert request.header("x-amz-date") is not None
            assert request.header("user-agent") is None
        finally:
            close_client(client)


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
        client = await create_async_client(
            config,
            auth=Auth.static_credentials("alternator", "secret"),
        )
        try:
            inject_custom_headers(client)
            captured = capture_prepared_requests(client)
            await client.list_tables()
            request = latest_request(captured)

            assert request.header("x-keep-me") == "keep"
            assert request.header("x-drop-me") is None
            assert request.header("authorization") is not None
            assert request.header("x-amz-date") is not None
        finally:
            await close_async_client(client)
