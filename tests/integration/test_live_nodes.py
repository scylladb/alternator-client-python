"""Integration tests for live-node discovery through public sessions."""

from __future__ import annotations

import pytest

from alternator import (
    Auth,
    Config,
    KeyRouteAffinityConfig,
    KeyRouteAffinityMode,
    Session,
)
from alternator.async_client import AsyncSession
from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


def _affinity_config() -> object:
    return Config(
        seed_hosts=[SCYLLA_HOST],
        port=SCYLLA_PORT,
        key_affinity=KeyRouteAffinityConfig(
            mode=KeyRouteAffinityMode.ANY_WRITE,
            table_pk_attributes={"preconfigured_table": "pk"},
        ),
    )


class TestLiveNodeSessionDiagnostics:
    """Verify sync session diagnostics and shared-client lifecycle."""

    def test_session_exposes_nodes_clients_and_partition_keys(self) -> None:
        with Session(_affinity_config(), auth=Auth.disabled()) as session:
            assert session.refresh_nodes()
            nodes = session.nodes
            assert nodes
            assert session.active_nodes == nodes
            assert session.quarantined_nodes == []
            assert session.supports_topology_filters()
            assert session.partition_key_for("preconfigured_table") == "pk"

            client = session.client("dynamodb")
            assert "TableNames" in client.list_tables()

            resource = session.resource("dynamodb")
            assert "TableNames" in resource.meta.client.list_tables()


class TestAsyncLiveNodeSessionDiagnostics:
    """Verify async session diagnostics and shared-client lifecycle."""

    @pytest.mark.asyncio
    async def test_session_exposes_nodes_clients_and_partition_keys(self) -> None:
        async with AsyncSession(_affinity_config(), auth=Auth.disabled()) as session:
            assert await session.refresh_nodes()
            nodes = session.nodes
            assert nodes
            assert session.active_nodes == nodes
            assert session.quarantined_nodes == []
            assert await session.supports_topology_filters()
            assert await session.partition_key_for("preconfigured_table") == "pk"

            client = await session.client("dynamodb")
            assert "TableNames" in await client.list_tables()
