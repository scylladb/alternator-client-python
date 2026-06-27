"""Integration tests for live-node discovery through public helpers."""

from __future__ import annotations

import pytest

from alternator import AlternatorConfigBuilder, Auth, KeyRouteAffinityMode, Session
from alternator.async_client import AsyncSession
from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


def _affinity_config() -> object:
    return (
        AlternatorConfigBuilder()
        .with_seeds(SCYLLA_HOST)
        .with_port(SCYLLA_PORT)
        .with_key_affinity(
            KeyRouteAffinityMode.ANY_WRITE,
            table_pk_map={"preconfigured_table": "pk"},
        )
        .build()
    )


class TestLiveNodeHelperDiagnostics:
    """Verify sync helper diagnostics and shared-client lifecycle."""

    def test_helper_exposes_nodes_clients_and_partition_keys(self) -> None:
        with Session(_affinity_config(), auth=Auth.disabled()) as helper:
            assert helper.refresh_nodes()
            nodes = helper.nodes
            assert nodes
            assert helper.active_nodes == nodes
            assert helper.quarantined_nodes == []
            assert helper.supports_topology_filters()
            assert helper.partition_key_for("preconfigured_table") == "pk"

            client = helper.client("dynamodb")
            assert "TableNames" in client.list_tables()

            resource = helper.resource("dynamodb")
            assert "TableNames" in resource.meta.client.list_tables()


class TestAsyncLiveNodeHelperDiagnostics:
    """Verify async helper diagnostics and shared-client lifecycle."""

    @pytest.mark.asyncio
    async def test_helper_exposes_nodes_clients_and_partition_keys(self) -> None:
        async with AsyncSession(_affinity_config(), auth=Auth.disabled()) as helper:
            assert await helper.refresh_nodes()
            nodes = helper.nodes
            assert nodes
            assert helper.active_nodes == nodes
            assert helper.quarantined_nodes == []
            assert await helper.supports_topology_filters()
            assert await helper.partition_key_for("preconfigured_table") == "pk"

            client = await helper.client("dynamodb")
            assert "TableNames" in await client.list_tables()
