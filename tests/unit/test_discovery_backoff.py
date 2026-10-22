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

"""Configured discovery policies reach both caches without sharing attempt state."""

from unittest.mock import AsyncMock, MagicMock

import pytest

import alternator.async_client as async_client_module
from alternator import (
    AlternatorConfigBuilder,
    Backoff,
    ExponentialBackoff,
    KeyRouteAffinityConfig,
    KeyRouteAffinityMode,
)
from alternator._constants import PK_CACHE_ATTR
from alternator.client import _create_affinity_node_computer
from alternator.core.key_affinity import PartitionKeyCache


class RecordingBackoff:
    """A structural custom policy that records one-based attempts."""

    def __init__(self) -> None:
        self.attempts: list[int] = []

    def delay_seconds(self, attempt: int) -> float:
        self.attempts.append(attempt)
        return 7.0


def test_policy_configuration() -> None:
    policy: Backoff = RecordingBackoff()
    assert KeyRouteAffinityConfig(discovery_backoff=policy).discovery_backoff is policy
    assert KeyRouteAffinityConfig().discovery_backoff == ExponentialBackoff()
    config = (
        AlternatorConfigBuilder()
        .with_seeds("localhost")
        .with_port(8000)
        .with_key_affinity(KeyRouteAffinityMode.RMW, discovery_backoff=policy)
        .build()
    )
    assert config.key_affinity.discovery_backoff is policy


@pytest.mark.parametrize("async_mode", [False, True])
async def test_configured_policy_tracks_attempts_per_table_and_cache(
    async_mode: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise actual factory wiring, cooldown boundaries and reset behavior."""
    clock = [100.0]
    monkeypatch.setattr("alternator.core.key_affinity.monotonic", lambda: clock[0])
    policy = RecordingBackoff()
    config = (
        AlternatorConfigBuilder()
        .with_seeds("localhost")
        .with_port(8000)
        .with_key_affinity(KeyRouteAffinityMode.RMW, discovery_backoff=policy)
        .build()
    )
    caches: list[PartitionKeyCache | async_client_module.AsyncPartitionKeyCache] = []

    async def lookup(
        cache: PartitionKeyCache | async_client_module.AsyncPartitionKeyCache,
        table: str,
    ) -> str | None:
        if isinstance(cache, async_client_module.AsyncPartitionKeyCache):
            return await cache.get_pk_name(table)
        return cache.get_pk_name(table)

    try:
        for _ in range(2):
            client = MagicMock()
            client.describe_table = (
                AsyncMock(return_value={}) if async_mode else MagicMock(return_value={})
            )
            if async_mode:
                client_context = MagicMock()
                client_context.__aenter__ = AsyncMock(return_value=client)
                session = MagicMock()
                session.client.return_value = client_context
                monkeypatch.setattr(
                    async_client_module, "_SdkSession", MagicMock(return_value=session)
                )
                monkeypatch.setattr(
                    async_client_module, "_register_alternator_handlers", MagicMock()
                )
                manager = MagicMock()
                manager.next_node_uri.return_value = "http://localhost:8000"
                await async_client_module._create_async_client_with_manager(
                    config, manager, owns_manager=False
                )
            else:
                assert _create_affinity_node_computer(config, client) is not None
            cache = getattr(client, PK_CACHE_ATTR)
            caches.append(cache)
            assert await lookup(cache, "table") is None
        assert policy.attempts == [1, 1]  # Shared policy, independent cache state.
        cache = caches[0]
        assert await lookup(cache, "other") is None
        assert policy.attempts == [1, 1, 1]
        clock[0] = 106.999
        assert await lookup(cache, "table") is None
        assert policy.attempts == [1, 1, 1]
        clock[0] = 107.0
        assert await lookup(cache, "table") is None
        assert policy.attempts == [1, 1, 1, 2]
        cache.preload({"table": "pk"})
        assert cache.get_cached_pk_name("table") == "pk"
        assert await lookup(cache, "table") == "pk"
        assert policy.attempts == [1, 1, 1, 2]
        if isinstance(cache, async_client_module.AsyncPartitionKeyCache):
            await cache.clear()
        else:
            cache.clear()
        assert await lookup(cache, "table") is None
        assert policy.attempts == [1, 1, 1, 2, 1]
    finally:
        for cache in caches:
            if isinstance(cache, async_client_module.AsyncPartitionKeyCache):
                await cache.close()
            else:
                cache.close()
