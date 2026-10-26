# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Connection reuse across clusters larger than urllib3's default host cache."""

from __future__ import annotations

import json
import socket
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from threading import Barrier, Event, get_ident
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import urlsplit

import pytest
from botocore.awsrequest import AWSRequest
from botocore.exceptions import HTTPClientError
from botocore.httpsession import ProxyConfiguration
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.exceptions import LocationParseError
from urllib3.response import HTTPResponse

from alternator.client import (
    _create_client_with_manager,
    _create_resource_with_manager,
    close_client,
)
from alternator.config import Config, RetryConfig, RetryMode
from alternator.core.live_nodes import SyncLiveNodesManager
from alternator.core.routing_scope import ClusterScope
from tests.fake_server import FakeAlternatorResponse, FakeAlternatorServer


@pytest.mark.parametrize("resource", [False, True])
@pytest.mark.parametrize("proxy", [False, True])
def test_host_pools_survive_topology_growth(resource: bool, proxy: bool) -> None:
    """Existing host pools survive routing, affinity, and discovery growth."""
    config = Config(seed_hosts=["seed.test"], port=8043, max_pool_connections=3)
    manager = SyncLiveNodesManager(config, lambda _: [])
    manager.set_fallback_nodes(["node0.test"], ClusterScope())
    factory = _create_resource_with_manager if resource else _create_client_with_manager
    owner: Any = factory(config, manager, owns_manager=False)
    client = owner.meta.client if resource else owner
    session = client._endpoint.http_session
    session._proxy_config = ProxyConfiguration(
        {"https": "http://proxy.test:8080"} if proxy else {}
    )
    pool_manager = (
        session._get_proxy_manager("http://proxy.test:8080")
        if proxy
        else session._manager
    )
    known_pools: dict[str, Any] = {}
    try:
        for size in (11, 22):
            nodes = [f"node{n}.test" for n in range(size)]
            # Ordinary routing and affinity may use different snapshots.
            manager.set_fallback_nodes(nodes[:5], ClusterScope())
            manager._core.update_affinity_nodes(nodes[5:], ClusterScope())
            for host in ["seed.test", *nodes]:
                pool = pool_manager.connection_from_url(f"https://{host}:8043")
                if host in known_pools:
                    assert pool is known_pools[host]
                known_pools[host] = pool
                assert pool.pool.maxsize == 3
            for host, pool in known_pools.items():
                assert pool_manager.connection_from_url(f"https://{host}:8043") is pool
    finally:
        close_client(owner)


@pytest.mark.parametrize("proxy", [False, True])
def test_unselected_malformed_host_preserves_valid_pools(proxy: bool) -> None:
    """An invalid affinity host cannot break a valid destination's cache miss."""
    nodes = [f"node{n}.test" for n in range(11)]
    config = Config(seed_hosts=[nodes[0]], port=8043, scheme="https")
    manager = SyncLiveNodesManager(config, lambda _: [])
    manager.set_fallback_nodes(nodes[:5], ClusterScope())
    manager._core.update_affinity_nodes([*nodes[5:], "[broken"], ClusterScope())
    owner: Any = _create_client_with_manager(config, manager, owns_manager=False)
    session = owner._endpoint.http_session
    proxy_url = "http://proxy.test:8080" if proxy else None
    session._proxy_config = ProxyConfiguration(
        {"https": proxy_url} if proxy_url else {}
    )
    try:
        pools = session._get_connection_manager("https://node0.test:8043/", proxy_url)
        current = {
            host: pools.connection_from_url(f"https://{host}:8043/") for host in nodes
        }
        pools.connection_from_url("https://retired.test:8043/")
        for host, pool in current.items():
            assert pools.connection_from_url(f"https://{host}:8043/") is pool
        with pytest.raises(LocationParseError):
            pools.connection_from_url("https://[broken:8043/")
    finally:
        close_client(owner)


@pytest.mark.parametrize("resource", [False, True])
def test_selected_destination_survives_topology_handoff(
    monkeypatch: pytest.MonkeyPatch, resource: bool
) -> None:
    """Discovery can replace a snapshot after a request selects its endpoint."""
    nodes = [f"node{n}.test" for n in range(11)]
    config = Config(seed_hosts=[nodes[0]], port=8043, scheme="https")
    manager = SyncLiveNodesManager(config, lambda _: [])
    manager.set_fallback_nodes(nodes, ClusterScope())
    factory = _create_resource_with_manager if resource else _create_client_with_manager
    owner: Any = factory(config, manager, owns_manager=False)
    client = owner.meta.client if resource else owner
    session = client._endpoint.http_session
    session._proxy_config = ProxyConfiguration({})
    pools = session._manager
    known_pools = {
        host: pools.connection_from_url(f"https://{host}:8043/") for host in nodes
    }
    retired = "retired0.test"

    def replace_topology(request: AWSRequest, **kwargs: object) -> None:
        assert urlsplit(request.url).hostname == retired
        manager.set_fallback_nodes(nodes, ClusterScope())

    def respond(
        self: HTTPSConnectionPool, method: str, url: str, **kwargs: object
    ) -> HTTPResponse:
        assert self.host == retired
        return HTTPResponse(
            body=BytesIO(b'{"TableNames": []}'), status=200, preload_content=False
        )

    client.meta.events.register_last("request-created.dynamodb.*", replace_topology)
    monkeypatch.setattr(HTTPSConnectionPool, "urlopen", respond)
    try:
        for retired in ("retired0.test", "retired1.test", "retired2.test"):
            manager.set_fallback_nodes([retired], ClusterScope())
            assert client.list_tables()["TableNames"] == []
        for host, pool in known_pools.items():
            assert pools.connection_from_url(f"https://{host}:8043/") is pool
    finally:
        close_client(owner)


@pytest.mark.parametrize("proxy", [False, True])
def test_current_pools_survive_concurrent_retired_destinations(proxy: bool) -> None:
    """Retired plans cannot evict normalized ordinary/affinity host pools."""
    nodes = ["[fe80::1%Eth0]", "Node0.test", "[2001:DB8::1]", "bücher.test"]
    nodes.extend(f"node{n}.test" for n in range(4, 11))
    config = Config(seed_hosts=[nodes[0]], port=8043, scheme="https")
    manager = SyncLiveNodesManager(config, lambda _: [])
    manager.set_fallback_nodes(nodes[:5], ClusterScope())
    manager._core.update_affinity_nodes(nodes[5:], ClusterScope())
    owner: Any = _create_client_with_manager(config, manager, owns_manager=False)
    session = owner._endpoint.http_session
    proxy_url = "http://proxy.test:8080" if proxy else None
    session._proxy_config = ProxyConfiguration(
        {"https": proxy_url} if proxy_url else {}
    )
    pools = session._get_connection_manager("https://node0.test:8043/", proxy_url)
    current = {
        host: pools.connection_from_url(f"https://{host}:8043/") for host in nodes
    }
    disposed: list[Any] = []
    pools.pools.dispose_func = disposed.append
    ready = Barrier(12, timeout=10)

    def visit(index: int) -> None:
        ready.wait()
        for attempt in range(4):
            pools.connection_from_url(f"https://retired{index}-{attempt}.test:8043/")
            host = nodes[index % len(nodes)]
            assert pools.connection_from_url(f"https://{host}:8043/") is current[host]

    try:
        with ThreadPoolExecutor(max_workers=12) as executor:
            list(executor.map(visit, range(12)))
        for host, pool in current.items():
            assert pools.connection_from_url(f"https://{host}:8043/") is pool
        assert len(pools.pools) == len(nodes) + 1
        assert disposed
        assert not any(pool in current.values() for pool in disposed)
    finally:
        ready.abort()
        close_client(owner)


@pytest.mark.parametrize("proxy", [False, True])
def test_waiting_lookup_preserves_replacement_topology_pools(
    monkeypatch: pytest.MonkeyPatch, proxy: bool
) -> None:
    """A waiting miss uses snapshots published before it acquires the cache lock."""
    old_nodes = [f"old{n}.test" for n in range(11)]
    new_nodes = [f"new{n}.test" for n in range(11)]
    config = Config(seed_hosts=[old_nodes[0]], port=8043, scheme="https")
    manager = SyncLiveNodesManager(config, lambda _: [])
    manager.set_fallback_nodes(old_nodes[:5], ClusterScope())
    manager._core.update_affinity_nodes(old_nodes[5:], ClusterScope())
    owner: Any = _create_client_with_manager(config, manager, owns_manager=False)
    session = owner._endpoint.http_session
    proxy_url = "http://proxy.test:8080" if proxy else None
    session._proxy_config = ProxyConfiguration(
        {"https": proxy_url} if proxy_url else {}
    )
    pool_manager = session._get_connection_manager(
        "https://old10.test:8043/", proxy_url
    )
    cache = pool_manager.pools
    original_lock = cache.lock
    waiting = Event()
    resume = Event()
    worker_id: int | None = None

    def acquire() -> None:
        if get_ident() == worker_id and not waiting.is_set():
            waiting.set()
            assert resume.wait(10)
        original_lock.__enter__()

    controlled_lock = MagicMock()
    controlled_lock.__enter__.side_effect = acquire
    controlled_lock.__exit__.side_effect = original_lock.__exit__
    monkeypatch.setattr(cache, "lock", controlled_lock)

    def visit_old_destination() -> None:
        nonlocal worker_id
        worker_id = get_ident()
        pool_manager.connection_from_url("https://old10.test:8043/")

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(visit_old_destination)
            try:
                assert waiting.wait(10)
                manager.set_fallback_nodes(new_nodes[:5], ClusterScope())
                manager._core.update_affinity_nodes(new_nodes[5:], ClusterScope())
                current = {
                    host: pool_manager.connection_from_url(f"https://{host}:8043/")
                    for host in [*new_nodes, old_nodes[0]]
                }
            finally:
                resume.set()
            pending.result(timeout=10)
        for host, pool in current.items():
            assert pool_manager.connection_from_url(f"https://{host}:8043/") is pool
    finally:
        resume.set()
        close_client(owner)


def test_proxy_setup_errors_preserve_sdk_mapping_and_standard_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Proxy construction stays inside the SDK's error handling and retry loop."""
    config = Config(
        seed_hosts=["node.test"],
        port=8043,
        scheme="https",
        retries=RetryConfig(max_attempts=2, mode=RetryMode.STANDARD),
    )
    manager = SyncLiveNodesManager(config, lambda _: [])
    manager.set_fallback_nodes(["node.test"], ClusterScope())
    owner: Any = _create_client_with_manager(config, manager, owns_manager=False)
    session = owner._endpoint.http_session
    session._proxy_config = ProxyConfiguration({"https": "http://proxy.test:99999"})
    observed: list[tuple[int, Exception | None]] = []

    def observe_retry(
        attempts: int, caught_exception: Exception | None, **kwargs: object
    ) -> None:
        observed.append((attempts, caught_exception))

    owner.meta.events.register_last("needs-retry.dynamodb.ListTables", observe_retry)
    sleep = MagicMock()
    monkeypatch.setattr("botocore.endpoint.time.sleep", sleep)
    try:
        with pytest.raises(HTTPClientError) as error:
            owner.list_tables()
        assert isinstance(error.value.kwargs["error"], LocationParseError)
        assert [attempt for attempt, _ in observed] == list(
            range(1, config.retries.max_attempts + 1)
        )
        assert all(isinstance(error, HTTPClientError) for _, error in observed)
        assert sleep.call_count == config.retries.max_attempts - 1
    finally:
        close_client(owner)


@pytest.mark.parametrize("resource", [False, True])
def test_concurrent_first_proxy_requests_size_pools_before_use(
    monkeypatch: pytest.MonkeyPatch, resource: bool
) -> None:
    """Initial overlapping sends use one fully sized lazy proxy manager."""
    nodes = [f"node{n}.test" for n in range(11)]
    config = Config(seed_hosts=nodes, port=8043, scheme="https", max_pool_connections=3)
    manager = SyncLiveNodesManager(config, lambda _: [])
    manager.set_fallback_nodes(nodes, ClusterScope())
    factory = _create_resource_with_manager if resource else _create_client_with_manager
    owner: Any = factory(config, manager, owns_manager=False)
    client = owner.meta.client if resource else owner
    session = client._endpoint.http_session
    proxy_url = "http://proxy.test:8080"
    session._proxy_config = ProxyConfiguration({"https": proxy_url})
    ready = Barrier(len(nodes), timeout=10)
    observed: dict[str, HTTPSConnectionPool] = {}

    def route(request: AWSRequest, **kwargs: object) -> None:
        host = json.loads(request.body)["ExclusiveStartTableName"]
        request.url = f"https://{host}:8043/"

    def before_send(**kwargs: object) -> None:
        ready.wait()

    def respond(
        self: HTTPSConnectionPool, method: str, url: str, **kwargs: object
    ) -> HTTPResponse:
        observed[self.host] = self
        assert self.pool is not None
        assert self.pool.maxsize == 3
        return HTTPResponse(
            body=BytesIO(b'{"TableNames": []}'), status=200, preload_content=False
        )

    client.meta.events.register_last("request-created.dynamodb.*", route)
    client.meta.events.register_last("before-send.dynamodb.*", before_send)
    monkeypatch.setattr(HTTPSConnectionPool, "urlopen", respond)
    try:
        assert session._proxy_managers == {}
        with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            results = list(
                executor.map(
                    lambda host: client.list_tables(ExclusiveStartTableName=host), nodes
                )
            )
        assert all(result["TableNames"] == [] for result in results)
        proxy_manager = session._proxy_managers[proxy_url]
        for host in nodes:
            assert (
                proxy_manager.connection_from_url(f"https://{host}:8043/")
                is observed[host]
            )
    finally:
        ready.abort()
        close_client(owner)


def test_configured_client_reconnects_after_server_close(
    monkeypatch: pytest.MonkeyPatch,
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """Sizing the selected manager leaves normal SDK retries and reconnects intact."""
    server = fake_alternator_server
    server.script(
        "ListTables",
        [FakeAlternatorResponse(200, b"", {}, disconnect=True)],
        fallback=FakeAlternatorResponse.json({"TableNames": []}),
    )
    port = urlsplit(server.url()).port
    assert port is not None
    monkeypatch.setenv("NO_PROXY", "*")
    monkeypatch.setenv("no_proxy", "*")
    config = Config(seed_hosts=["127.0.0.1"], port=port)
    manager = SyncLiveNodesManager(config, lambda _: [])
    manager.set_fallback_nodes(["127.0.0.1"], ClusterScope())
    owner = _create_client_with_manager(config, manager, owns_manager=False)
    try:
        assert owner.list_tables()["TableNames"] == []
        assert owner.list_tables()["TableNames"] == []
    finally:
        close_client(owner)
    requests = server.captured_requests(operation="ListTables")
    assert len(requests) == 3
    assert requests[0].connection_id != requests[1].connection_id
    assert requests[1].connection_id == requests[2].connection_id


@pytest.mark.parametrize("resource", [False, True])
def test_requests_reuse_tcp_connections_across_eleven_hosts(
    monkeypatch: pytest.MonkeyPatch,
    fake_alternator_server: FakeAlternatorServer,
    resource: bool,
) -> None:
    """Two passes through eleven hosts need eleven TCP connections, not 22."""
    server = fake_alternator_server
    server.set_operation("ListTables", {"TableNames": []})
    port = urlsplit(server.url()).port
    assert port is not None
    nodes = [f"node{n}.test" for n in range(11)]
    original_resolve = socket.getaddrinfo

    def resolve(
        host: bytes | str | None,
        port: bytes | str | int | None,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[Any]:
        return original_resolve("127.0.0.1", port, family, type, proto, flags)

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    monkeypatch.setenv("NO_PROXY", "*")
    monkeypatch.setenv("no_proxy", "*")
    config = Config(seed_hosts=nodes, port=port)
    manager = SyncLiveNodesManager(config, lambda _: [])
    manager.set_fallback_nodes(nodes, ClusterScope())
    factory = _create_resource_with_manager if resource else _create_client_with_manager
    owner: Any = factory(config, manager, owns_manager=False)
    client = owner.meta.client if resource else owner
    destinations = iter(nodes * 2)

    def route(request: AWSRequest, **kwargs: object) -> None:
        request.url = f"http://{next(destinations)}:{port}/"

    client.meta.events.register_last("request-created.dynamodb.ListTables", route)
    try:
        for _ in range(22):
            assert client.list_tables()["TableNames"] == []
    finally:
        close_client(owner)

    requests = server.captured_requests(operation="ListTables")
    assert len(requests) == 22
    assert len({request.connection_id for request in requests}) == 11
