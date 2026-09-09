# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for AsyncLiveNodesManager and AsyncPartitionKeyCache."""

import asyncio
import gc
import warnings
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

import alternator.async_client as async_client_module
from alternator._constants import MANAGER_ATTR, MANAGER_OWNS_ATTR, PK_CACHE_ATTR
from alternator.config import Config
from alternator.core.live_nodes import AsyncLiveNodesManager, NoNodesAvailableError
from alternator.core.routing_scope import ClusterScope, DatacenterScope, RackScope
from alternator.exceptions import ConfigurationError


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
        assert cast(object, manager._refresh_task) is not None

        # Stop
        await manager.stop()
        assert cast(object, manager._refresh_task) is None

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
        """Test explicit fallback to broader scope when narrow scope returns empty."""
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
            routing_scope=DatacenterScope("dc1", fallback=ClusterScope()),
        )

        manager = AsyncLiveNodesManager(config, mock_fetch)
        result = await manager.refresh_nodes()

        assert result is True
        # Should have tried dc scope first, then fallen back to cluster
        assert len(call_urls) == 2
        assert "dc=dc1" in call_urls[0]
        assert "dc=" not in call_urls[1]

    @pytest.mark.asyncio
    async def test_cluster_scope_aggregates_all_seed_localnodes(self) -> None:
        """Cluster scope unions local-DC node lists returned by each seed."""
        call_urls: list[str] = []

        async def mock_fetch(url: str) -> list[str]:
            call_urls.append(url)
            if "seed-dc1" in url:
                return ["dc1-node1", "dc1-node2"]
            if "seed-dc2" in url:
                return ["dc2-node1", "dc2-node2"]
            return []

        config = Config(
            seed_hosts=["seed-dc1", "seed-dc2"],
            port=8000,
            routing_scope=ClusterScope(),
        )
        manager = AsyncLiveNodesManager(config, mock_fetch)

        assert await manager.refresh_nodes() is True
        assert call_urls == [
            "http://seed-dc1:8000/localnodes",
            "http://seed-dc2:8000/localnodes",
        ]
        assert manager.nodes.nodes == (
            "dc1-node1",
            "dc1-node2",
            "dc2-node1",
            "dc2-node2",
        )

    @pytest.mark.asyncio
    async def test_cluster_scope_aggregates_reachable_seed_localnodes(self) -> None:
        """Cluster scope keeps usable seed results when another seed fails."""

        async def mock_fetch(url: str) -> list[str]:
            if "seed-dc1" in url:
                raise OSError("seed unavailable")
            return ["dc2-node1", "dc2-node2"]

        config = Config(
            seed_hosts=["seed-dc1", "seed-dc2"],
            port=8000,
            routing_scope=ClusterScope(),
        )
        manager = AsyncLiveNodesManager(config, mock_fetch)

        assert await manager.refresh_nodes() is True
        assert manager.nodes.nodes == ("dc2-node1", "dc2-node2")

    @pytest.mark.asyncio
    async def test_url_construction(self, config: Config) -> None:
        """Test build_localnodes_url constructs correct URL."""

        async def empty_fetch(url: str) -> list[str]:
            return []

        manager = AsyncLiveNodesManager(config, empty_fetch)

        url = manager._core.build_localnodes_url(ClusterScope(), "192.168.1.1")
        assert url == "http://192.168.1.1:8000/localnodes"

    @pytest.mark.asyncio
    async def test_url_construction_brackets_ipv6_seed(self) -> None:
        """Test raw IPv6 seed hosts are bracketed in discovery URLs."""

        async def empty_fetch(url: str) -> list[str]:
            return []

        config = Config(seed_hosts=["2001:db8::1"], port=8000)
        manager = AsyncLiveNodesManager(config, empty_fetch)

        url = manager._core.build_localnodes_url(ClusterScope(), "2001:db8::1")
        assert url == "http://[2001:db8::1]:8000/localnodes"

    @pytest.mark.asyncio
    async def test_next_node_uri_brackets_ipv6_node(self) -> None:
        """Test raw IPv6 live nodes are bracketed in operation URLs."""

        async def mock_fetch(url: str) -> list[str]:
            return ["2001:db8::2"]

        config = Config(seed_hosts=["seed"], port=8000)
        manager = AsyncLiveNodesManager(config, mock_fetch)
        await manager.refresh_nodes()

        assert manager.next_node_uri() == "http://[2001:db8::2]:8000"

    @pytest.mark.asyncio
    async def test_refresh_recovers_through_original_ipv6_entrypoint(self) -> None:
        """Later refreshes keep using the original seed after nodes are learned."""
        config = Config(seed_hosts=["2001:db8::1"], port=8000)
        calls: list[str] = []

        async def mock_fetch(url: str) -> list[str]:
            calls.append(url)
            if len(calls) == 1:
                return ["2001:db8::2"]
            if len(calls) == 2:
                return []
            return ["2001:db8::3"]

        manager = AsyncLiveNodesManager(config, mock_fetch)

        assert await manager.refresh_nodes() is True
        assert manager.nodes.nodes == ("2001:db8::2",)
        assert await manager.refresh_nodes() is False
        assert manager.nodes.nodes == ("2001:db8::2",)
        assert await manager.refresh_nodes() is True
        assert manager.nodes.nodes == ("2001:db8::3",)
        assert calls == [
            "http://[2001:db8::1]:8000/localnodes",
            "http://[2001:db8::1]:8000/localnodes",
            "http://[2001:db8::1]:8000/localnodes",
        ]

    @pytest.mark.asyncio
    async def test_refresh_recovers_through_original_dns_entrypoint(self) -> None:
        """Failed refresh retains nodes, then recovers through logical seed host."""
        config = Config(seed_hosts=["entrypoint.test"], port=8000)
        calls: list[str] = []

        async def mock_fetch(url: str) -> list[str]:
            calls.append(url)
            if len(calls) == 1:
                return ["old-node.test"]
            if len(calls) == 2:
                return []
            return ["new-node.test"]

        manager = AsyncLiveNodesManager(config, mock_fetch)

        assert await manager.refresh_nodes() is True
        assert manager.nodes.nodes == ("old-node.test",)
        assert await manager.refresh_nodes() is False
        assert manager.nodes.nodes == ("old-node.test",)
        assert await manager.refresh_nodes() is True
        assert manager.nodes.nodes == ("new-node.test",)
        assert calls == [
            "http://entrypoint.test:8000/localnodes",
            "http://entrypoint.test:8000/localnodes",
            "http://entrypoint.test:8000/localnodes",
        ]

    @pytest.mark.asyncio
    async def test_url_construction_with_dc_scope(self, config: Config) -> None:
        """Test URL construction with datacenter scope."""

        async def empty_fetch(url: str) -> list[str]:
            return []

        manager = AsyncLiveNodesManager(config, empty_fetch)

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

    @pytest.mark.asyncio
    async def test_topology_validation_uses_scope_without_state_update(self) -> None:
        """Validation queries one scoped endpoint without replacing live nodes."""
        call_urls: list[str] = []

        async def mock_fetch(url: str) -> list[str]:
            call_urls.append(url)
            if "dc=dc1" in url:
                return ["node1"]
            return []

        config = Config(
            seed_hosts=["seed1"],
            port=8000,
            routing_scope=DatacenterScope("dc1", fallback=None),
        )
        manager = AsyncLiveNodesManager(config, mock_fetch)

        assert await manager.check_rack_and_datacenter_set_correctly() is True
        assert await manager.check_rack_datacenter_feature_supported() is True
        assert manager.nodes.nodes == ()
        assert call_urls == [
            "http://seed1:8000/localnodes?dc=dc1",
            "http://seed1:8000/localnodes?dc=dc1",
        ]

    @pytest.mark.asyncio
    async def test_topology_validation_raises_for_missing_scope(self) -> None:
        """Validation raises a clear error when no scoped nodes exist."""

        async def mock_fetch(url: str) -> list[str]:
            return []

        config = Config(
            seed_hosts=["seed1"],
            port=8000,
            routing_scope=RackScope("dc1", "missing", fallback=None),
        )
        manager = AsyncLiveNodesManager(config, mock_fetch)

        assert await manager.check_rack_datacenter_feature_supported() is False
        with pytest.raises(ConfigurationError, match="No nodes found"):
            await manager.check_rack_and_datacenter_set_correctly()


class TestAsyncPartitionKeyCache:
    """Tests for AsyncPartitionKeyCache."""

    @pytest.mark.asyncio
    async def test_preload_returns_cached_value(self) -> None:
        """Test that preloaded values are returned without fetch."""
        client = MagicMock()
        cache = async_client_module.AsyncPartitionKeyCache(client)

        cache.preload({"my_table": "pk"})

        result = await cache.get_pk_name("my_table")
        assert result == "pk"
        # Should not have called describe_table
        client.describe_table.assert_not_called()

    def test_cached_lookup_without_running_loop_does_not_leak_coroutine(self) -> None:
        """Synchronous cache probes fail quietly when no event loop is active."""
        cache = async_client_module.AsyncPartitionKeyCache(MagicMock())

        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always", RuntimeWarning)
            assert cache.get_cached_pk_name("table") is None
            gc.collect()

        assert not [warning for warning in seen if warning.category is RuntimeWarning]

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

        cache = async_client_module.AsyncPartitionKeyCache(client)

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

        cache = async_client_module.AsyncPartitionKeyCache(client)

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

        cache = async_client_module.AsyncPartitionKeyCache(client)

        result = await cache.get_pk_name("test_table")
        assert result is None

    @pytest.mark.asyncio
    async def test_clear_removes_all_cached(self) -> None:
        """Test that clear removes all cached values."""
        client = AsyncMock()
        client.describe_table.return_value = {
            "Table": {"KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]}
        }

        cache = async_client_module.AsyncPartitionKeyCache(client)
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

        async def slow_describe_table(TableName: str) -> dict[str, Any]:
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

        cache = async_client_module.AsyncPartitionKeyCache(client)

        # Launch multiple concurrent requests
        tasks = [asyncio.create_task(cache.get_pk_name("test_table")) for _ in range(5)]

        # Wait for all to complete
        results = await asyncio.gather(*tasks)

        # All should return the same value
        assert all(r == "id" for r in results)
        # But only one fetch should have happened
        assert fetch_count == 1

    @pytest.mark.asyncio
    async def test_cancelled_owner_wakes_waiter_and_allows_retry(self) -> None:
        """Cancellation clears pending discovery so a later call can retry."""
        fetch_started = asyncio.Event()
        never_complete = asyncio.Event()
        fetch_count = 0

        async def describe_table(TableName: str) -> dict[str, Any]:
            nonlocal fetch_count
            fetch_count += 1
            if fetch_count == 1:
                fetch_started.set()
                await never_complete.wait()
            return {
                "Table": {"KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]}
            }

        client = AsyncMock()
        client.describe_table = describe_table
        cache = async_client_module.AsyncPartitionKeyCache(client)

        owner = asyncio.create_task(cache.get_pk_name("test_table"))
        await fetch_started.wait()
        waiter = asyncio.create_task(cache.get_pk_name("test_table"))
        await asyncio.sleep(0)
        assert not waiter.done()

        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner

        assert await asyncio.wait_for(waiter, timeout=1) is None
        assert "test_table" not in cache._pending
        assert await cache.get_pk_name("test_table") == "id"
        assert fetch_count == 2

    @pytest.mark.asyncio
    async def test_different_tables_fetch_independently(self) -> None:
        """Test that different tables are fetched independently."""
        client = AsyncMock()
        client.describe_table.return_value = {
            "Table": {"KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]}
        }

        cache = async_client_module.AsyncPartitionKeyCache(client)

        await cache.get_pk_name("table1")
        await cache.get_pk_name("table2")

        assert client.describe_table.call_count == 2

    @pytest.mark.asyncio
    async def test_background_discovery_is_coalesced_and_cancelled_on_close(
        self,
    ) -> None:
        """Request-path misses own one cancellable task per table."""
        fetch_started = asyncio.Event()
        fetch_cancelled = asyncio.Event()
        fetch_count = 0

        async def describe_table(TableName: str) -> dict[str, Any]:
            nonlocal fetch_count
            assert TableName == "test_table"
            fetch_count += 1
            fetch_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                fetch_cancelled.set()
            return {}

        client = AsyncMock()
        client.describe_table = describe_table
        cache = async_client_module.AsyncPartitionKeyCache(client)

        for _ in range(25):
            assert cache.get_cached_pk_name("test_table") is None
        await fetch_started.wait()

        assert fetch_count == 1
        assert len(cache._background_tasks) == 1

        await cache.close()

        assert fetch_cancelled.is_set()
        assert cache._background_tasks == {}
        assert cache.get_cached_pk_name("test_table") is None


@pytest.mark.asyncio
async def test_create_manager_closes_fetcher_when_initial_refresh_cancelled(
    config: Config,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelled initial discovery closes its lazily-created HTTP session."""

    class BlockingFetcher:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.closed = asyncio.Event()

        async def __call__(self, url: str) -> list[str]:
            self.started.set()
            await asyncio.Event().wait()
            return []

        async def close(self) -> None:
            self.closed.set()

    fetcher = BlockingFetcher()

    def create_fetcher(*args: object, **kwargs: object) -> BlockingFetcher:
        return fetcher

    monkeypatch.setattr(
        async_client_module, "create_async_http_fetcher", create_fetcher
    )

    create_task = asyncio.create_task(async_client_module._create_async_manager(config))
    await fetcher.started.wait()
    create_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await create_task
    assert fetcher.closed.is_set()


@pytest.mark.asyncio
async def test_repeated_cancellation_cannot_interrupt_manager_cleanup(
    config: Config,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second cancellation still lets discovery cleanup finish."""

    class BlockingCloseFetcher:
        def __init__(self) -> None:
            self.fetch_started = asyncio.Event()
            self.close_started = asyncio.Event()
            self.allow_close = asyncio.Event()
            self.closed = asyncio.Event()

        async def __call__(self, url: str) -> list[str]:
            self.fetch_started.set()
            await asyncio.Event().wait()
            return []

        async def close(self) -> None:
            self.close_started.set()
            await self.allow_close.wait()
            self.closed.set()

    fetcher = BlockingCloseFetcher()

    def create_fetcher(*args: object, **kwargs: object) -> BlockingCloseFetcher:
        return fetcher

    monkeypatch.setattr(
        async_client_module,
        "create_async_http_fetcher",
        create_fetcher,
    )

    create_task = asyncio.create_task(async_client_module._create_async_manager(config))
    await fetcher.fetch_started.wait()
    create_task.cancel()
    await fetcher.close_started.wait()
    create_task.cancel()
    fetcher.allow_close.set()

    with pytest.raises(asyncio.CancelledError):
        await create_task
    assert fetcher.closed.is_set()


@pytest.mark.asyncio
async def test_create_client_closes_manager_when_cancelled(
    config: Config,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation during SDK client creation releases the manager."""
    manager = AsyncMock(spec=AsyncLiveNodesManager)
    client_creation_started = asyncio.Event()

    async def create_manager(config: Config) -> AsyncLiveNodesManager:
        return cast(AsyncLiveNodesManager, manager)

    async def create_client(*args: object, **kwargs: object) -> object:
        client_creation_started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    close_manager = AsyncMock()
    monkeypatch.setattr(async_client_module, "_create_async_manager", create_manager)
    monkeypatch.setattr(
        async_client_module, "_create_async_client_with_manager", create_client
    )
    monkeypatch.setattr(async_client_module, "_close_async_manager", close_manager)

    create_task = asyncio.create_task(async_client_module.create_async_client(config))
    await client_creation_started.wait()
    create_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await create_task
    manager.start.assert_awaited_once_with()
    close_manager.assert_awaited_once_with(manager)


@pytest.mark.asyncio
async def test_entered_sdk_client_closes_when_setup_is_cancelled(
    config: Config,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation after entering SDK context closes that context."""
    manager = MagicMock(spec=AsyncLiveNodesManager)
    manager.next_node_uri.return_value = "http://127.0.0.1:8000"
    client = MagicMock()
    client.meta.events = MagicMock()
    client_context = MagicMock()
    client_context.__aenter__ = AsyncMock(return_value=client)
    client_context.__aexit__ = AsyncMock(return_value=None)
    session = MagicMock()
    session.client.return_value = client_context

    monkeypatch.setattr(async_client_module, "_SdkSession", lambda: session)

    def cancel_setup(*args: object, **kwargs: object) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(
        async_client_module,
        "_register_alternator_handlers",
        cancel_setup,
    )

    with pytest.raises(asyncio.CancelledError):
        await async_client_module._create_async_client_with_manager(
            config,
            manager,
            owns_manager=True,
        )

    client_context.__aenter__.assert_awaited_once_with()
    client_context.__aexit__.assert_awaited_once_with(None, None, None)


@pytest.mark.asyncio
async def test_close_async_client_stops_partition_key_discovery() -> None:
    """Client closure stops its background partition-key tasks first."""
    client = MagicMock()
    client.__aexit__ = AsyncMock(return_value=None)
    cache = MagicMock()
    cache.close = AsyncMock(return_value=None)
    setattr(client, MANAGER_ATTR, MagicMock())
    setattr(client, MANAGER_OWNS_ATTR, False)
    setattr(client, PK_CACHE_ATTR, cache)

    await async_client_module.close_async_client(client)

    cache.close.assert_awaited_once_with()
    assert getattr(client, PK_CACHE_ATTR) is None
    client.__aexit__.assert_awaited_once_with(None, None, None)


@pytest.mark.asyncio
async def test_cancelled_close_still_releases_all_client_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation cannot skip cache, manager, or SDK cleanup."""
    cache_close_started = asyncio.Event()
    allow_cache_close = asyncio.Event()
    client = MagicMock()
    client.__aexit__ = AsyncMock(return_value=None)
    cache = MagicMock()
    manager = MagicMock()

    async def close_cache() -> None:
        cache_close_started.set()
        await allow_cache_close.wait()

    cache.close = close_cache
    setattr(client, PK_CACHE_ATTR, cache)
    setattr(client, MANAGER_ATTR, manager)
    setattr(client, MANAGER_OWNS_ATTR, True)
    close_manager = AsyncMock()
    monkeypatch.setattr(async_client_module, "_close_async_manager", close_manager)

    close_task = asyncio.create_task(async_client_module.close_async_client(client))
    await cache_close_started.wait()
    close_task.cancel()
    await asyncio.sleep(0)
    close_task.cancel()
    await asyncio.sleep(0)
    allow_cache_close.set()

    with pytest.raises(asyncio.CancelledError):
        await close_task

    close_manager.assert_awaited_once_with(manager)
    assert getattr(client, PK_CACHE_ATTR) is None
    assert getattr(client, MANAGER_ATTR) is None
    assert getattr(client, MANAGER_OWNS_ATTR) is False
    client.__aexit__.assert_awaited_once_with(None, None, None)
