"""Tests for AsyncLiveNodesManager and AsyncPartitionKeyCache."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from alternator.async_client import AsyncPartitionKeyCache
from alternator.config import Config
from alternator.core.live_nodes import AsyncLiveNodesManager, NoNodesAvailableError
from alternator.core.routing_scope import ClusterScope, DatacenterScope


@pytest.fixture
def config() -> Config:
    """Create test config."""
    return Config(
        seed_hosts=["192.168.1.1"],
        port=8000,
        scheme="http",
    )


class TestAsyncLiveNodesManager:
    """Tests for AsyncLiveNodesManager."""

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self, config: Config) -> None:
        """Test manager start/stop lifecycle."""

        async def mock_fetch(url: str) -> list[str]:
            return ["192.168.1.1", "192.168.1.2"]

        manager = AsyncLiveNodesManager(config, mock_fetch)

        # Initial state
        assert manager._refresh_task is None

        # Start
        await manager.start()
        assert manager._refresh_task is not None

        # Stop
        await manager.stop()
        assert manager._refresh_task is None

    @pytest.mark.asyncio
    async def test_next_node_uri_format(self, config: Config) -> None:
        """Test next_node_uri returns correctly formatted URI."""

        async def mock_fetch(url: str) -> list[str]:
            return ["192.168.1.1", "192.168.1.2"]

        manager = AsyncLiveNodesManager(config, mock_fetch)
        await manager.refresh_nodes()

        uri = manager.next_node_uri()
        assert uri.startswith("http://")
        assert ":8000" in uri

    @pytest.mark.asyncio
    async def test_next_node_uri_raises_when_empty(self, config: Config) -> None:
        """Test next_node_uri raises when no nodes available."""

        async def mock_fetch(url: str) -> list[str]:
            return []

        manager = AsyncLiveNodesManager(config, mock_fetch)

        with pytest.raises(NoNodesAvailableError):
            manager.next_node_uri()

    @pytest.mark.asyncio
    async def test_fallback_chain_on_empty(self) -> None:
        """Test fallback to broader scope when narrow scope returns empty."""
        call_urls: list[str] = []

        async def mock_fetch(url: str) -> list[str]:
            call_urls.append(url)
            # Return empty for datacenter scope, nodes for cluster scope
            if "dc=" in url:
                return []
            return ["192.168.1.1"]

        config = Config(
            seed_hosts=["192.168.1.1"],
            port=8000,
            routing_scope=DatacenterScope(datacenter="dc1"),
        )

        manager = AsyncLiveNodesManager(config, mock_fetch)
        result = await manager.refresh_nodes()

        assert result is True
        # Should have tried dc scope first, then fallen back to cluster
        assert len(call_urls) == 2
        assert "dc=dc1" in call_urls[0]
        assert "dc=" not in call_urls[1]

    @pytest.mark.asyncio
    async def test_url_construction(self, config: Config) -> None:
        """Test build_localnodes_url constructs correct URL."""
        manager = AsyncLiveNodesManager(config, lambda _: [])

        url = manager._core.build_localnodes_url(ClusterScope(), "192.168.1.1")
        assert url == "http://192.168.1.1:8000/localnodes"

    @pytest.mark.asyncio
    async def test_url_construction_with_dc_scope(self, config: Config) -> None:
        """Test URL construction with datacenter scope."""
        manager = AsyncLiveNodesManager(config, lambda _: [])

        url = manager._core.build_localnodes_url(
            DatacenterScope(datacenter="dc1"), "192.168.1.1"
        )
        assert url == "http://192.168.1.1:8000/localnodes?dc=dc1"

    @pytest.mark.asyncio
    async def test_round_robin_selection(self, config: Config) -> None:
        """Test round-robin selection works."""

        async def mock_fetch(url: str) -> list[str]:
            return ["a", "b", "c"]

        manager = AsyncLiveNodesManager(config, mock_fetch)
        await manager.refresh_nodes()

        # Get several nodes and verify round-robin
        nodes = [manager._core.next_node() for _ in range(6)]
        assert nodes == ["a", "b", "c", "a", "b", "c"]

    @pytest.mark.asyncio
    async def test_background_refresh_updates_nodes(self, config: Config) -> None:
        """Test that background refresh updates node list."""
        call_count = 0

        async def mock_fetch(url: str) -> list[str]:
            nonlocal call_count
            call_count += 1
            return [f"node{call_count}"]

        # Config with fast refresh
        from alternator.config import NodeListPollingConfig

        fast_config = Config(
            seed_hosts=["192.168.1.1"],
            port=8000,
            node_list_polling=NodeListPollingConfig(active_interval_ms=50),
        )

        manager = AsyncLiveNodesManager(fast_config, mock_fetch)

        # Initial fetch
        await manager.refresh_nodes()
        initial_nodes = manager.nodes.nodes

        # Start background refresh
        await manager.start()

        # Wait for at least one background refresh
        await asyncio.sleep(0.1)

        # Stop and verify nodes changed
        await manager.stop()

        # Should have refreshed at least once
        assert call_count >= 2
        assert manager.nodes.nodes != initial_nodes

    @pytest.mark.asyncio
    async def test_refresh_failure_keeps_existing_nodes(self, config: Config) -> None:
        """Test that refresh failure preserves existing node list."""
        first_call = True

        async def mock_fetch(url: str) -> list[str]:
            nonlocal first_call
            if first_call:
                first_call = False
                return ["node1", "node2"]
            return []  # Simulates failure

        manager = AsyncLiveNodesManager(config, mock_fetch)

        # First fetch succeeds
        await manager.refresh_nodes()
        initial_nodes = manager.nodes.nodes
        assert len(initial_nodes) == 2

        # Second fetch fails but nodes remain
        await manager.refresh_nodes()
        assert manager.nodes.nodes == initial_nodes


class TestAsyncPartitionKeyCache:
    """Tests for AsyncPartitionKeyCache."""

    @pytest.mark.asyncio
    async def test_preload_returns_cached_value(self) -> None:
        """Test that preloaded values are returned without fetch."""
        client = MagicMock()
        cache = AsyncPartitionKeyCache(client)

        cache.preload({"my_table": "pk"})

        result = await cache.get_pk_name("my_table")
        assert result == "pk"
        # Should not have called describe_table
        client.describe_table.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetch_and_cache(self) -> None:
        """Test fetching from DescribeTable and caching result."""
        client = AsyncMock()
        client.describe_table.return_value = {
            "Table": {
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ]
            }
        }

        cache = AsyncPartitionKeyCache(client)

        # First call should fetch
        result = await cache.get_pk_name("test_table")
        assert result == "id"
        client.describe_table.assert_called_once_with(TableName="test_table")

        # Second call should use cache
        client.describe_table.reset_mock()
        result = await cache.get_pk_name("test_table")
        assert result == "id"
        client.describe_table.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetch_returns_none_on_error(self) -> None:
        """Test that fetch errors return None."""
        client = AsyncMock()
        client.describe_table.side_effect = Exception("Network error")

        cache = AsyncPartitionKeyCache(client)

        result = await cache.get_pk_name("test_table")
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_returns_none_when_no_hash_key(self) -> None:
        """Test that None is returned when table has no HASH key."""
        client = AsyncMock()
        client.describe_table.return_value = {
            "Table": {
                "KeySchema": [
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ]
            }
        }

        cache = AsyncPartitionKeyCache(client)

        result = await cache.get_pk_name("test_table")
        assert result is None

    @pytest.mark.asyncio
    async def test_clear_removes_all_cached(self) -> None:
        """Test that clear removes all cached values."""
        client = AsyncMock()
        client.describe_table.return_value = {
            "Table": {"KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]}
        }

        cache = AsyncPartitionKeyCache(client)
        cache.preload({"table1": "pk1", "table2": "pk2"})

        await cache.clear()

        # Should need to fetch again
        await cache.get_pk_name("table1")
        client.describe_table.assert_called_once_with(TableName="table1")

    @pytest.mark.asyncio
    async def test_concurrent_requests_single_fetch(self) -> None:
        """Test that concurrent requests for same table only fetch once."""
        fetch_count = 0
        fetch_event = asyncio.Event()

        async def slow_describe_table(TableName: str) -> dict:
            nonlocal fetch_count
            fetch_count += 1
            # Wait a bit to simulate network delay
            await asyncio.sleep(0.05)
            fetch_event.set()
            return {
                "Table": {"KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]}
            }

        client = AsyncMock()
        client.describe_table = slow_describe_table

        cache = AsyncPartitionKeyCache(client)

        # Launch multiple concurrent requests
        tasks = [asyncio.create_task(cache.get_pk_name("test_table")) for _ in range(5)]

        # Wait for all to complete
        results = await asyncio.gather(*tasks)

        # All should return the same value
        assert all(r == "id" for r in results)
        # But only one fetch should have happened
        assert fetch_count == 1

    @pytest.mark.asyncio
    async def test_different_tables_fetch_independently(self) -> None:
        """Test that different tables are fetched independently."""
        client = AsyncMock()
        client.describe_table.return_value = {
            "Table": {"KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]}
        }

        cache = AsyncPartitionKeyCache(client)

        await cache.get_pk_name("table1")
        await cache.get_pk_name("table2")

        assert client.describe_table.call_count == 2
