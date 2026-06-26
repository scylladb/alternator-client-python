"""Integration tests for live-node discovery through public helpers."""

from __future__ import annotations

import pytest

from alternator import AlternatorConfigBuilder, Auth, Helper, KeyRouteAffinityMode
from alternator.async_client import AsyncHelper
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
        with Helper(_affinity_config(), auth=Auth.disabled()) as helper:
            assert helper.update_live_nodes()
            nodes = helper.get_nodes()
            assert nodes
            assert helper.get_active_nodes() == nodes
            assert helper.get_quarantined_nodes() == []
            assert helper.check_rack_datacenter_feature_supported()
            assert helper.get_partition_key_name("preconfigured_table") == "pk"

            visited = {helper.next_node() for _ in range(len(nodes) * 2)}
            assert set(nodes).issubset({node for node in visited if node is not None})

            client = helper.client()
            assert "TableNames" in client.list_tables()

            resource = helper.resource()
            assert "TableNames" in resource.meta.client.list_tables()


class TestAsyncLiveNodeHelperDiagnostics:
    """Verify async helper diagnostics and shared-client lifecycle."""

    @pytest.mark.asyncio
    async def test_helper_exposes_nodes_clients_and_partition_keys(self) -> None:
        async with AsyncHelper(_affinity_config(), auth=Auth.disabled()) as helper:
            assert await helper.update_live_nodes()
            nodes = helper.get_nodes()
            assert nodes
            assert helper.get_active_nodes() == nodes
            assert helper.get_quarantined_nodes() == []
            assert await helper.check_rack_datacenter_feature_supported()
            assert await helper.get_partition_key_name("preconfigured_table") == "pk"

            visited: set[str] = set()
            for _ in range(len(nodes) * 2):
                node = await helper.next_node()
                assert node is not None
                visited.add(node)
            assert set(nodes).issubset(visited)

            client = await helper.client()
            assert "TableNames" in await client.list_tables()
