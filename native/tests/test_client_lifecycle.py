# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Explicit wheel tests for the native constructor and lifecycle boundary."""

from __future__ import annotations

import asyncio
import inspect
import struct
import subprocess
import sys
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlsplit

import _alternator_rust as native
import pytest

from alternator import Config
from alternator._experimental.rust._config import prepare_config
from alternator.core.routing_scope import (
    ClusterScope,
    DatacenterScope,
    RackScope,
    RoutingScope,
)
from tests.fake_server import FakeAlternatorResponse, FakeAlternatorServer


@contextmanager
def running_server() -> Iterator[FakeAlternatorServer]:
    server = FakeAlternatorServer()
    server.start()
    try:
        yield server
    finally:
        try:
            assert not [
                request
                for request in server.captured_requests(operation="Scan")
                if request.table
                and request.table.startswith(".scylla.alternator.system.")
            ], "native topology discovery must use GET /localnodes"
        finally:
            server.stop()


def mapping(
    server: FakeAlternatorServer, scope: RoutingScope | None = None
) -> dict[str, object]:
    config = Config(["127.0.0.1"], urlsplit(server.url()).port or 0)
    if scope is not None:
        config = replace(config, routing_scope=scope)
    return prepare_config(config).native_config()


def test_invalid_native_mapping_fails_before_discovery() -> None:
    with running_server() as server:
        config = mapping(server)
        for change in (
            {"unknown_setting": True},
            {"schema_version": 2},
            {"port": True},
            {"read_seconds": True},
            {"read_seconds": float("nan")},
            {"read_seconds": float("inf")},
            {"read_seconds": 0},
            {"read_seconds": 1e-20},
            {"read_seconds": float(2**64)},
            {"active_interval_ms": 0},
            {"active_interval_ms": 2**64},
            {"idle_interval_ms": 2**64},
            {"compression_min_bytes": -1},
            {"compression_min_bytes": 2 ** (8 * struct.calcsize("P"))},
            {"max_attempts": 0},
            {"max_attempts": 2**32},
            {"scopes": []},
            {"scopes": [("datacenter", None, None)]},
            {"ca_pem": [b"not PEM"]},
            {"system_roots": False},
        ):
            with pytest.raises(ValueError):
                native.create_client(config | change)
        assert server.captured_requests() == []


def test_native_constructor_accepts_values_above_previous_caps() -> None:
    scope: RoutingScope = ClusterScope()
    for index in range(64):
        scope = DatacenterScope(f"dc-{index}", fallback=scope)
    with running_server() as server:
        server.set_json("/localnodes?dc=dc-63", ["127.0.0.1"])
        config = mapping(server, scope) | {
            "active_interval_ms": 2**31,
            "idle_interval_ms": 2**31,
            "compression_min_bytes": 2**31,
            "max_attempts": 2**31,
            "connect_seconds": float(2**32),
            "read_seconds": float(2**32),
            "discovery_seconds": float(2**32),
        }
        client = native.create_client(config)
        try:
            requests = server.wait_for_requests(1, path="/localnodes?dc=dc-63")
            assert requests[0].method == "GET"
        finally:
            client.close()


def test_native_constructor_accepts_control_characters_in_partition_key_name() -> None:
    with running_server() as server:
        server.set_localnodes(["127.0.0.1"])
        config = Config(["127.0.0.1"], urlsplit(server.url()).port or 0)
        config = replace(
            config,
            key_affinity=replace(
                config.key_affinity, table_pk_attributes={"table": "pk\nname"}
            ),
        )
        client = native.create_client(prepare_config(config).native_config())
        try:
            requests = server.wait_for_requests(1, path="/localnodes")
            assert requests[0].method == "GET"
        finally:
            client.close()
        assert client.closed
        assert client._live_drivers() == 0


@pytest.mark.parametrize(
    "scope,paths",
    [
        (ClusterScope(), ["/localnodes"]),
        (DatacenterScope("dc one"), ["/localnodes?dc=dc+one"]),
        (RackScope("dc one", "r/1"), ["/localnodes?dc=dc+one&rack=r%2F1"]),
        (
            RackScope("dc one", "r/1", fallback=DatacenterScope("dc one")),
            ["/localnodes?dc=dc+one&rack=r%2F1", "/localnodes?dc=dc+one"],
        ),
    ],
    ids=["cluster", "dc", "rack", "rack-dc-fallback"],
)
def test_native_constructor_discovers_scoped_localnodes(
    scope: RoutingScope, paths: list[str]
) -> None:
    with running_server() as server:
        for path in paths[:-1]:
            server.set_json(path, [])
        server.set_json(paths[-1], ["127.0.0.1"])
        client = native.create_client(mapping(server, scope))
        try:
            server.wait_for_requests(1, path=paths[-1])
            requests = server.captured_requests()
            assert [request.path for request in requests[: len(paths)]] == paths
            assert all(request.method == "GET" for request in requests)
        finally:
            client.close()


def test_sync_close_drains_held_discovery_and_preserves_shared_runtime(
    tmp_path: Path,
) -> None:
    # Bound native close and executor cleanup even if either deadlocks. Resolve
    # production imports before adding only the parent's shared fixture location.
    subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "".join(
                [
                    "import alternator, _alternator_rust, runpy, sys; ",
                    "sys.path.insert(0, sys.argv[2]); ",
                    "runpy.run_path(sys.argv[1])['_sync_close_held_discovery']()",
                ]
            ),
            str(Path(__file__).resolve()),
            str(Path(inspect.getfile(FakeAlternatorServer)).resolve().parents[1]),
        ],
        cwd=tmp_path,
        check=True,
        timeout=20,
    )


def _sync_close_held_discovery() -> None:
    entered, release = threading.Event(), threading.Event()
    with running_server() as server:
        server.set_localnodes(["127.0.0.1"])
        server.script_get(
            "/localnodes",
            [
                FakeAlternatorResponse.json(
                    ["127.0.0.1"], entered=entered, release=release
                )
            ],
        )
        client = native.create_client(mapping(server))
        try:
            assert entered.wait(3)
            requests = server.wait_for_requests(1, path="/localnodes")
            assert requests[0].method == "GET"
            runtime = native._runtime_id()
            with ThreadPoolExecutor(max_workers=3) as workers:
                try:
                    closes = [workers.submit(client.close) for _ in range(3)]
                    for closed in closes:
                        closed.result(timeout=3)
                finally:
                    # Release held I/O before executor shutdown joins close workers.
                    release.set()
            assert client.closed
            assert client._live_drivers() == 0
            client.close()
            assert native._runtime_id() == runtime
        finally:
            release.set()
            client.close()


def test_async_cancelled_close_waiter_does_not_cancel_cleanup(tmp_path: Path) -> None:
    # Bound the entire child, including event-loop and native shutdown on failure.
    subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "".join(
                [
                    "import alternator, _alternator_rust, runpy, sys; ",
                    "sys.path.insert(0, sys.argv[2]); ",
                    "runpy.run_path(sys.argv[1])['_async_cancelled_close_waiter']()",
                ]
            ),
            str(Path(__file__).resolve()),
            str(Path(inspect.getfile(FakeAlternatorServer)).resolve().parents[1]),
        ],
        cwd=tmp_path,
        check=True,
        timeout=20,
    )


def _async_cancelled_close_waiter() -> None:
    async def run(server: FakeAlternatorServer) -> None:
        first = await native.create_client_async(mapping(server))
        second = await native.create_client_async(mapping(server))
        try:
            await asyncio.to_thread(server.wait_for_requests, 2, path="/localnodes")
            closing = first.close_async()
            closing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await closing
            await asyncio.wait_for(
                asyncio.gather(first.close_async(), first.close_async()), timeout=3
            )
            assert first.closed
            assert first._live_drivers() == 0
            assert not second.closed
            assert await native._runtime_id_async() == native._runtime_id()
            # A Python callback queued beside Rust work still runs on its loop.
            progressed = asyncio.Event()
            asyncio.get_running_loop().call_soon(progressed.set)
            await asyncio.wait_for(progressed.wait(), timeout=3)
        finally:
            await asyncio.wait_for(
                asyncio.gather(second.close_async(), first.close_async()), timeout=3
            )

    with running_server() as server:
        server.set_localnodes(["127.0.0.1"])
        asyncio.run(run(server))


def test_async_client_rejects_another_loop() -> None:
    async def create(server: FakeAlternatorServer) -> object:
        client = await native.create_client_async(mapping(server))
        try:
            await asyncio.to_thread(server.wait_for_requests, 1, path="/localnodes")
            return client
        except BaseException:
            await client.close_async()
            raise

    async def wrong_loop(client: object) -> None:
        with pytest.raises(RuntimeError, match="different event loop"):
            client.close_async()  # type: ignore[attr-defined]

    with running_server() as server:
        server.set_localnodes(["127.0.0.1"])
        client = asyncio.run(create(server))
        try:
            asyncio.run(wrong_loop(client))
        finally:
            client.close()  # type: ignore[attr-defined]
