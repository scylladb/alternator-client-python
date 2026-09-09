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

"""Refresh concurrency, cancellation, and shutdown regressions."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Sequence

import pytest

from alternator.config import Config, NodeListPollingConfig, TimeoutConfig
from alternator.core.live_nodes import AsyncLiveNodesManager, SyncLiveNodesManager
from alternator.core.routing_scope import ClusterScope


def test_overlapping_sync_refreshes_are_serialized_and_atomic() -> None:
    """Readers see complete old/new snapshots while sync refreshes overlap."""
    entered = threading.Event()
    release = threading.Event()
    calls = 0
    errors: list[Exception] = []

    def fetch(url: str) -> Sequence[str]:
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            assert release.wait(timeout=2)
            return ["127.0.0.2", "127.0.0.3"]
        return ["127.0.0.4", "127.0.0.5"]

    manager = SyncLiveNodesManager(Config(seed_hosts=["seed.test"], port=8000), fetch)
    manager.set_fallback_nodes(["127.0.0.1"], ClusterScope())

    def refresh() -> None:
        try:
            assert manager.refresh_nodes() is True
        except Exception as error:
            errors.append(error)

    first = threading.Thread(target=refresh)
    second = threading.Thread(target=refresh)
    first.start()
    assert entered.wait(timeout=1)
    second.start()
    time.sleep(0.05)

    assert calls == 1
    assert manager.nodes.nodes == ("127.0.0.1",)

    release.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert calls == 2
    assert list(manager.nodes.nodes) == ["127.0.0.4", "127.0.0.5"]


@pytest.mark.asyncio
async def test_overlapping_async_refreshes_are_serialized_and_atomic() -> None:
    """Readers see complete old/new snapshots while async refreshes overlap."""
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def fetch(url: str) -> Sequence[str]:
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await asyncio.wait_for(release.wait(), timeout=2)
            return ["127.0.0.2", "127.0.0.3"]
        return ["127.0.0.4", "127.0.0.5"]

    manager = AsyncLiveNodesManager(Config(seed_hosts=["seed.test"], port=8000), fetch)
    manager.set_fallback_nodes(["127.0.0.1"], ClusterScope())

    first = asyncio.create_task(manager.refresh_nodes())
    await asyncio.wait_for(entered.wait(), timeout=1)
    second = asyncio.create_task(manager.refresh_nodes())
    await asyncio.sleep(0)

    assert calls == 1
    assert manager.nodes.nodes == ("127.0.0.1",)

    release.set()
    assert await first is True
    assert await second is True
    assert calls == 2
    assert list(manager.nodes.nodes) == ["127.0.0.4", "127.0.0.5"]


@pytest.mark.asyncio
async def test_async_stop_cancels_in_flight_discovery() -> None:
    """Async shutdown cancels blocked discovery without leaking its task."""
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def fetch(url: str) -> Sequence[str]:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        return []

    manager = AsyncLiveNodesManager(Config(seed_hosts=["seed.test"], port=8000), fetch)
    await manager.start()
    await asyncio.wait_for(entered.wait(), timeout=1)

    await asyncio.wait_for(manager.stop(), timeout=1)

    assert cancelled.is_set()
    assert manager._refresh_task is None


def test_sync_stop_is_bounded_and_restart_waits_for_stopped_thread() -> None:
    """A blocked refresh thread must exit before a replacement can start."""
    entered = threading.Event()
    release = threading.Event()
    restarted = threading.Event()
    calls = 0

    def fetch(url: str) -> Sequence[str]:
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            release.wait(timeout=3)
        else:
            restarted.set()
        return []

    config = Config(
        seed_hosts=["seed.test"],
        port=8000,
        timeouts=TimeoutConfig(discovery_seconds=0.05),
    )
    manager = SyncLiveNodesManager(config, fetch)
    manager.start()
    assert entered.wait(timeout=1)

    started = time.monotonic()
    manager.stop()
    elapsed = time.monotonic() - started

    assert elapsed < 1.5
    assert manager._refresh_thread is not None
    assert manager._refresh_thread.is_alive()

    stopped_thread = manager._refresh_thread
    for _ in range(3):
        manager.start()
        assert manager._refresh_thread is stopped_thread

    release.set()
    stopped_thread.join(timeout=1)
    assert not stopped_thread.is_alive()

    manager.start()
    assert manager._refresh_thread is not stopped_thread
    assert restarted.wait(timeout=1)
    manager.stop()
    assert manager._refresh_thread is None


def test_failed_sync_background_refresh_does_not_busy_loop() -> None:
    """Repeated failures wait for configured polling interval."""
    calls = 0

    def fetch(url: str) -> Sequence[str]:
        nonlocal calls
        calls += 1
        return []

    config = Config(
        seed_hosts=["seed.test"],
        port=8000,
        node_list_polling=NodeListPollingConfig(
            active_interval_ms=50,
            idle_interval_ms=50,
        ),
    )
    manager = SyncLiveNodesManager(config, fetch)
    manager.start()
    time.sleep(0.16)
    manager.stop()

    assert 2 <= calls <= 5


def test_sync_activity_wakes_idle_refresh_loop() -> None:
    """Resumed traffic does not wait for remaining idle polling delay."""
    refreshed_twice = threading.Event()
    calls = 0

    def fetch(url: str) -> Sequence[str]:
        nonlocal calls
        calls += 1
        if calls == 2:
            refreshed_twice.set()
        return ["127.0.0.1"]

    config = Config(
        seed_hosts=["seed.test"],
        port=8000,
        node_list_polling=NodeListPollingConfig(
            active_interval_ms=20,
            idle_interval_ms=1000,
        ),
    )
    manager = SyncLiveNodesManager(config, fetch)
    manager._core._last_activity = time.monotonic() - 2
    manager.start()
    try:
        deadline = time.monotonic() + 1
        while calls < 1 and time.monotonic() < deadline:
            time.sleep(0.005)
        assert calls == 1

        assert manager.next_node_uri() == "http://127.0.0.1:8000"

        assert refreshed_twice.wait(timeout=0.2)
    finally:
        manager.stop()


def test_sync_sustained_activity_does_not_trigger_refresh_per_request() -> None:
    """Active traffic keeps configured polling cadence instead of busy-looping."""
    calls = 0

    def fetch(url: str) -> Sequence[str]:
        nonlocal calls
        calls += 1
        return ["127.0.0.1"]

    config = Config(
        seed_hosts=["seed.test"],
        port=8000,
        node_list_polling=NodeListPollingConfig(
            active_interval_ms=50,
            idle_interval_ms=1000,
        ),
    )
    manager = SyncLiveNodesManager(config, fetch)
    manager.start()
    try:
        deadline = time.monotonic() + 0.16
        while time.monotonic() < deadline:
            manager.mark_activity()
            time.sleep(0.001)
    finally:
        manager.stop()

    assert 2 <= calls <= 6


@pytest.mark.asyncio
async def test_failed_async_background_refresh_does_not_busy_loop() -> None:
    """Async failures wait for configured polling interval."""
    calls = 0

    async def fetch(url: str) -> Sequence[str]:
        nonlocal calls
        calls += 1
        return []

    config = Config(
        seed_hosts=["seed.test"],
        port=8000,
        node_list_polling=NodeListPollingConfig(
            active_interval_ms=50,
            idle_interval_ms=50,
        ),
    )
    manager = AsyncLiveNodesManager(config, fetch)
    await manager.start()
    await asyncio.sleep(0.16)
    await manager.stop()

    assert 2 <= calls <= 5


@pytest.mark.asyncio
async def test_async_activity_wakes_idle_refresh_loop() -> None:
    """Async traffic wakes discovery from its idle polling delay."""
    refreshed_twice = asyncio.Event()
    calls = 0

    async def fetch(url: str) -> Sequence[str]:
        nonlocal calls
        calls += 1
        if calls == 2:
            refreshed_twice.set()
        return ["127.0.0.1"]

    config = Config(
        seed_hosts=["seed.test"],
        port=8000,
        node_list_polling=NodeListPollingConfig(
            active_interval_ms=20,
            idle_interval_ms=1000,
        ),
    )
    manager = AsyncLiveNodesManager(config, fetch)
    manager._core._last_activity = time.monotonic() - 2
    await manager.start()
    try:
        deadline = asyncio.get_running_loop().time() + 1
        while calls < 1 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.005)
        assert calls == 1

        assert manager.next_node_uri() == "http://127.0.0.1:8000"

        await asyncio.wait_for(refreshed_twice.wait(), timeout=0.2)
    finally:
        await manager.stop()


@pytest.mark.parametrize(
    ("selection_method", "expected"),
    (
        ("next_node", "127.0.0.1"),
        ("next_node_uri", "http://127.0.0.1:8000"),
    ),
)
@pytest.mark.asyncio
async def test_async_worker_thread_activity_wakes_idle_refresh_loop(
    selection_method: str,
    expected: str,
) -> None:
    """Worker-thread selection safely wakes an idle asyncio refresh loop."""
    refreshed_twice = asyncio.Event()
    calls = 0

    async def fetch(url: str) -> Sequence[str]:
        nonlocal calls
        calls += 1
        if calls == 2:
            refreshed_twice.set()
        return ["127.0.0.1"]

    config = Config(
        seed_hosts=["seed.test"],
        port=8000,
        node_list_polling=NodeListPollingConfig(
            active_interval_ms=20,
            idle_interval_ms=1000,
        ),
    )
    manager = AsyncLiveNodesManager(config, fetch)
    manager._core._last_activity = time.monotonic() - 2
    loop = asyncio.get_running_loop()
    previous_debug = loop.get_debug()
    loop.set_debug(True)
    await manager.start()
    try:
        deadline = loop.time() + 1
        while not manager._wake_event._waiters and loop.time() < deadline:
            await asyncio.sleep(0.005)
        assert manager._wake_event._waiters

        result = await asyncio.to_thread(getattr(manager, selection_method))

        assert result == expected
        await asyncio.wait_for(refreshed_twice.wait(), timeout=0.2)
    finally:
        await manager.stop()
        loop.set_debug(previous_debug)


@pytest.mark.asyncio
async def test_async_sustained_activity_does_not_trigger_refresh_per_request() -> None:
    """Async active traffic retains configured polling cadence."""
    calls = 0

    async def fetch(url: str) -> Sequence[str]:
        nonlocal calls
        calls += 1
        return ["127.0.0.1"]

    config = Config(
        seed_hosts=["seed.test"],
        port=8000,
        node_list_polling=NodeListPollingConfig(
            active_interval_ms=50,
            idle_interval_ms=1000,
        ),
    )
    manager = AsyncLiveNodesManager(config, fetch)
    await manager.start()
    try:
        deadline = asyncio.get_running_loop().time() + 0.16
        while asyncio.get_running_loop().time() < deadline:
            manager.mark_activity()
            await asyncio.sleep(0.001)
    finally:
        await manager.stop()

    assert 2 <= calls <= 6
