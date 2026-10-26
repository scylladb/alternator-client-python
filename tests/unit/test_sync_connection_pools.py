# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Connection reuse across clusters larger than urllib3's default host cache."""

from __future__ import annotations

import socket
from typing import Any
from urllib.parse import urlsplit

import pytest
from botocore.awsrequest import AWSRequest

from alternator.client import (
    _create_client_with_manager,
    _create_resource_with_manager,
    close_client,
)
from alternator.config import Config
from alternator.core.live_nodes import SyncLiveNodesManager
from alternator.core.routing_scope import ClusterScope
from tests.fake_server import FakeAlternatorServer


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
            client.meta.events.emit(
                "before-send.dynamodb.ListTables",
                request=AWSRequest(
                    method="POST", url="http://node0.test:8043/"
                ).prepare(),
            )
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
