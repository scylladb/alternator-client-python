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

"""DNS contact-host fallback and recovery regressions."""

from __future__ import annotations

import json
import os
import socket
import time
from collections.abc import Sequence
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Event, Thread
from typing import cast
from unittest.mock import patch

import pytest

from alternator import Auth, Helper
from alternator._http import AsyncNodeFetcher, create_sync_http_fetcher
from alternator.async_client import AsyncHelper
from alternator.config import Config, NodeListPollingConfig
from alternator.core.live_nodes import AsyncLiveNodesManager, SyncLiveNodesManager
from alternator.core.routing_scope import ClusterScope, DatacenterScope

AddressInfo = tuple[int, int, int, str, tuple[str, int]]


def _address_info(address: str, port: int) -> list[AddressInfo]:
    return [
        (
            socket.AF_INET,
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP,
            "",
            (address, port),
        )
    ]


def _handler(
    status: int,
    body: bytes,
    hosts: list[str] | None = None,
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if hosts is not None:
                hosts.append(self.headers["Host"])
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def _contact_handler(
    nodes: list[str],
    hosts: list[str],
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            hosts.append(self.headers["Host"])
            body = json.dumps(nodes).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def _dynamodb_handler(requests: list[str]) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            requests.append(self.headers.get("X-Amz-Target", ""))
            body = b'{"TableNames":[]}'
            self.send_response(200)
            self.send_header("Content-Type", "application/x-amz-json-1.0")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def _start_server(
    address: str,
    port: int,
    handler: type[BaseHTTPRequestHandler],
) -> tuple[HTTPServer, Thread]:
    try:
        server = HTTPServer((address, port), handler)
    except OSError as error:
        pytest.skip(f"loopback alias {address} is unavailable: {error}")
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _stop_server(server: HTTPServer, thread: Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


@pytest.mark.parametrize(
    ("status", "body"),
    (
        (503, b'["127.0.0.9"]'),
        (200, b"not-json"),
        (200, b"[]"),
        (200, b'["node.example.com"]'),
    ),
    ids=("non-2xx", "malformed", "empty", "unusable"),
)
def test_sync_bad_contact_host_advances_to_next_configured_host(
    status: int,
    body: bytes,
) -> None:
    """Bad /localnodes data advances by configured host, not resolved address."""
    bad, bad_thread = _start_server("127.0.0.1", 0, _handler(status, body))
    good_body = json.dumps(["127.0.0.9"]).encode()
    good, good_thread = _start_server(
        "127.0.0.2", bad.server_port, _handler(200, good_body)
    )
    resolutions: list[str] = []

    def resolve(
        host: str, port: int, *args: object, **kwargs: object
    ) -> list[AddressInfo]:
        resolutions.append(host)
        address = {"bad.test": "127.0.0.1", "good.test": "127.0.0.2"}[host]
        return _address_info(address, port)

    config = Config(
        seed_hosts=["bad.test", "good.test"],
        port=bad.server_port,
        routing_scope=DatacenterScope("dc1"),
    )
    manager = SyncLiveNodesManager(
        config, create_sync_http_fetcher(timeout_seconds=1.0)
    )
    try:
        with (
            patch.dict(os.environ, {"NO_PROXY": "*", "no_proxy": "*"}),
            patch("socket.getaddrinfo", side_effect=resolve),
        ):
            assert manager.refresh_nodes() is True
    finally:
        _stop_server(bad, bad_thread)
        _stop_server(good, good_thread)

    assert resolutions == ["bad.test", "good.test"]
    assert list(manager.nodes.nodes) == ["127.0.0.9"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "body"),
    (
        (503, b'["127.0.0.9"]'),
        (200, b"not-json"),
        (200, b"[]"),
        (200, b'["node.example.com"]'),
    ),
    ids=("non-2xx", "malformed", "empty", "unusable"),
)
async def test_async_bad_contact_host_advances_to_next_configured_host(
    status: int,
    body: bytes,
) -> None:
    """aiohttp applies invalid-response fallback between configured hosts."""
    pytest.importorskip("aiohttp")
    bad, bad_thread = _start_server("127.0.0.1", 0, _handler(status, body))
    good_body = json.dumps(["127.0.0.9"]).encode()
    good, good_thread = _start_server(
        "127.0.0.2", bad.server_port, _handler(200, good_body)
    )
    resolutions: list[str] = []

    def resolve(
        host: str, port: int, *args: object, **kwargs: object
    ) -> list[AddressInfo]:
        resolutions.append(host)
        address = {"bad.test": "127.0.0.1", "good.test": "127.0.0.2"}[host]
        return _address_info(address, port)

    fetcher = AsyncNodeFetcher(timeout_seconds=1.0)
    config = Config(
        seed_hosts=["bad.test", "good.test"],
        port=bad.server_port,
        routing_scope=DatacenterScope("dc1"),
    )
    manager = AsyncLiveNodesManager(config, fetcher)
    try:
        with patch("socket.getaddrinfo", side_effect=resolve):
            assert await manager.refresh_nodes() is True
    finally:
        await fetcher.close()
        _stop_server(bad, bad_thread)
        _stop_server(good, good_thread)

    assert resolutions == ["bad.test", "good.test"]
    assert list(manager.nodes.nodes) == ["127.0.0.9"]


def test_sync_stalled_dns_seed_does_not_block_next_configured_host() -> None:
    """A bounded DNS lookup lets discovery advance to another seed host."""
    body = json.dumps(["127.0.0.9"]).encode()
    server, thread = _start_server("127.0.0.1", 0, _handler(200, body))
    release = Event()
    stalled_lookup_finished = Event()

    def resolve(
        host: str, port: int, *args: object, **kwargs: object
    ) -> list[AddressInfo]:
        if host == "stalled.test":
            try:
                release.wait(timeout=2)
            finally:
                stalled_lookup_finished.set()
            raise socket.gaierror(socket.EAI_AGAIN, "temporary failure")
        assert host == "good.test"
        return _address_info("127.0.0.1", port)

    manager = SyncLiveNodesManager(
        Config(
            seed_hosts=["stalled.test", "good.test"],
            port=server.server_port,
            routing_scope=DatacenterScope("dc1"),
        ),
        create_sync_http_fetcher(timeout_seconds=0.05),
    )
    try:
        with (
            patch.dict(os.environ, {"NO_PROXY": "*", "no_proxy": "*"}),
            patch("socket.getaddrinfo", side_effect=resolve),
        ):
            started = time.monotonic()
            assert manager.refresh_nodes() is True
            elapsed = time.monotonic() - started

            assert elapsed < 0.5
            assert list(manager.nodes.nodes) == ["127.0.0.9"]
    finally:
        release.set()
        assert stalled_lookup_finished.wait(timeout=1)
        _stop_server(server, thread)


@pytest.mark.parametrize(
    "first_result",
    (
        socket.gaierror(socket.EAI_NONAME, "not found"),
        socket.gaierror(socket.EAI_AGAIN, "temporary failure"),
        TimeoutError("DNS timed out"),
        [],
    ),
    ids=("nxdomain", "servfail", "timeout", "empty-answer"),
)
def test_sync_dns_failure_retains_seed_and_recovers_on_reresolution(
    first_result: BaseException | list[AddressInfo],
) -> None:
    """Later sync refresh resolves the original seed after DNS failure."""
    body = json.dumps(["127.0.0.9"]).encode()
    hosts: list[str] = []
    server, thread = _start_server("127.0.0.1", 0, _handler(200, body, hosts))
    attempts = 0

    def resolve(
        host: str, port: int, *args: object, **kwargs: object
    ) -> list[AddressInfo]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            if isinstance(first_result, BaseException):
                raise first_result
            return first_result
        return _address_info("127.0.0.1", port)

    config = Config(seed_hosts=["entrypoint.test"], port=server.server_port)
    manager = SyncLiveNodesManager(
        config, create_sync_http_fetcher(timeout_seconds=1.0)
    )
    try:
        with (
            patch.dict(os.environ, {"NO_PROXY": "*", "no_proxy": "*"}),
            patch("socket.getaddrinfo", side_effect=resolve),
        ):
            assert manager.refresh_nodes() is False
            assert manager.nodes.nodes == ()
            assert manager.refresh_nodes() is True
    finally:
        _stop_server(server, thread)

    assert attempts == 2
    assert hosts == [f"entrypoint.test:{server.server_port}"]
    assert list(manager.nodes.nodes) == ["127.0.0.9"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "first_result",
    (
        socket.gaierror(socket.EAI_NONAME, "not found"),
        socket.gaierror(socket.EAI_AGAIN, "temporary failure"),
        TimeoutError("DNS timed out"),
        [],
    ),
    ids=("nxdomain", "servfail", "timeout", "empty-answer"),
)
async def test_async_dns_failure_retains_seed_and_recovers_on_reresolution(
    first_result: BaseException | list[AddressInfo],
) -> None:
    """Later aiohttp refresh resolves the original seed after DNS failure."""
    pytest.importorskip("aiohttp")
    body = json.dumps(["127.0.0.9"]).encode()
    hosts: list[str] = []
    server, thread = _start_server("127.0.0.1", 0, _handler(200, body, hosts))
    attempts = 0

    def resolve(
        host: str, port: int, *args: object, **kwargs: object
    ) -> list[AddressInfo]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            if isinstance(first_result, BaseException):
                raise first_result
            return first_result
        return _address_info("127.0.0.1", port)

    fetcher = AsyncNodeFetcher(timeout_seconds=1.0)
    config = Config(seed_hosts=["entrypoint.test"], port=server.server_port)
    manager = AsyncLiveNodesManager(config, fetcher)
    try:
        with patch("socket.getaddrinfo", side_effect=resolve):
            assert await manager.refresh_nodes() is False
            assert manager.nodes.nodes == ()
            assert await manager.refresh_nodes() is True
    finally:
        await fetcher.close()
        _stop_server(server, thread)

    assert attempts == 2
    assert hosts == [f"entrypoint.test:{server.server_port}"]
    assert list(manager.nodes.nodes) == ["127.0.0.9"]


def test_sync_active_session_uses_changed_dns_answer() -> None:
    """A later sync refresh uses a changed DNS answer for retained seed."""
    first_body = json.dumps(["127.0.0.10"]).encode()
    second_body = json.dumps(["127.0.0.11"]).encode()
    first, first_thread = _start_server("127.0.0.1", 0, _handler(200, first_body))
    second, second_thread = _start_server(
        "127.0.0.2", first.server_port, _handler(200, second_body)
    )
    answers = iter(("127.0.0.1", "127.0.0.2"))

    def resolve(
        host: str, port: int, *args: object, **kwargs: object
    ) -> list[AddressInfo]:
        assert host == "entrypoint.test"
        return _address_info(next(answers), port)

    manager = SyncLiveNodesManager(
        Config(seed_hosts=["entrypoint.test"], port=first.server_port),
        create_sync_http_fetcher(timeout_seconds=1.0),
    )
    try:
        with (
            patch.dict(os.environ, {"NO_PROXY": "*", "no_proxy": "*"}),
            patch("socket.getaddrinfo", side_effect=resolve),
        ):
            assert manager.refresh_nodes() is True
            assert manager.nodes.nodes == ("127.0.0.10",)
            assert manager.refresh_nodes() is True
    finally:
        _stop_server(first, first_thread)
        _stop_server(second, second_thread)

    assert manager.nodes.nodes == ("127.0.0.11",)


@pytest.mark.asyncio
async def test_async_active_session_uses_changed_dns_answer() -> None:
    """A later connection uses a changed DNS answer for the retained seed."""
    pytest.importorskip("aiohttp")
    first_body = json.dumps(["127.0.0.10"]).encode()
    second_body = json.dumps(["127.0.0.11"]).encode()
    first, first_thread = _start_server("127.0.0.1", 0, _handler(200, first_body))
    second, second_thread = _start_server(
        "127.0.0.2", first.server_port, _handler(200, second_body)
    )
    answers = iter(("127.0.0.1", "127.0.0.2"))

    def resolve(
        host: str, port: int, *args: object, **kwargs: object
    ) -> list[AddressInfo]:
        assert host == "entrypoint.test"
        return _address_info(next(answers), port)

    fetcher = AsyncNodeFetcher(timeout_seconds=1.0)
    manager = AsyncLiveNodesManager(
        Config(seed_hosts=["entrypoint.test"], port=first.server_port),
        fetcher,
    )
    try:
        with patch("socket.getaddrinfo", side_effect=resolve):
            assert await manager.refresh_nodes() is True
            assert manager.nodes.nodes == ("127.0.0.10",)
            assert await manager.refresh_nodes() is True
    finally:
        await fetcher.close()
        _stop_server(first, first_thread)
        _stop_server(second, second_thread)

    assert manager.nodes.nodes == ("127.0.0.11",)


def test_operations_continue_after_contact_host_recovery() -> None:
    """Operations move from failed learned IP to replacement learned via seed."""
    contact_nodes = ["127.0.0.2"]
    contact_hosts: list[str] = []
    old_requests: list[str] = []
    new_requests: list[str] = []
    contact, contact_thread = _start_server(
        "127.0.0.1",
        0,
        _contact_handler(contact_nodes, contact_hosts),
    )
    old, old_thread = _start_server(
        "127.0.0.2",
        contact.server_port,
        _dynamodb_handler(old_requests),
    )
    new, new_thread = _start_server(
        "127.0.0.3",
        contact.server_port,
        _dynamodb_handler(new_requests),
    )
    original_getaddrinfo = socket.getaddrinfo

    def resolve(
        host: bytes | str | None,
        port: int | str | None,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[AddressInfo]:
        if host == "entrypoint.test":
            assert isinstance(port, int)
            return _address_info("127.0.0.1", port)
        return cast(
            list[AddressInfo],
            original_getaddrinfo(host, port, family, type, proto, flags),
        )

    config = Config(
        seed_hosts=["entrypoint.test"],
        port=contact.server_port,
        node_list_polling=NodeListPollingConfig(
            active_interval_ms=60_000,
            idle_interval_ms=60_000,
        ),
    )
    try:
        with (
            patch.dict(os.environ, {"NO_PROXY": "*", "no_proxy": "*"}),
            patch("socket.getaddrinfo", side_effect=resolve),
            Helper(config, auth=Auth.disabled()) as helper,
        ):
            client = helper.client()
            assert client.list_tables()["TableNames"] == []
            assert old_requests == ["DynamoDB_20120810.ListTables"]

            _stop_server(old, old_thread)
            contact_nodes[:] = ["127.0.0.3"]
            assert helper.update_live_nodes() is True
            assert helper.get_nodes() == ["127.0.0.3"]

            assert client.list_tables()["TableNames"] == []
            assert new_requests == ["DynamoDB_20120810.ListTables"]
    finally:
        if old_thread.is_alive():
            _stop_server(old, old_thread)
        _stop_server(contact, contact_thread)
        _stop_server(new, new_thread)

    assert contact_hosts
    assert set(contact_hosts) == {f"entrypoint.test:{contact.server_port}"}


def test_operation_retries_another_learned_ip_after_partial_failure() -> None:
    """A failed learned IP does not prevent routing to another learned IP."""
    contact_nodes = ["127.0.0.2", "127.0.0.3"]
    contact_hosts: list[str] = []
    good_requests: list[str] = []
    contact, contact_thread = _start_server(
        "127.0.0.1",
        0,
        _contact_handler(contact_nodes, contact_hosts),
    )
    good, good_thread = _start_server(
        "127.0.0.3",
        contact.server_port,
        _dynamodb_handler(good_requests),
    )
    original_getaddrinfo = socket.getaddrinfo

    def resolve(
        host: bytes | str | None,
        port: int | str | None,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[AddressInfo]:
        if host == "entrypoint.test":
            assert isinstance(port, int)
            return _address_info("127.0.0.1", port)
        return cast(
            list[AddressInfo],
            original_getaddrinfo(host, port, family, type, proto, flags),
        )

    config = Config(
        seed_hosts=["entrypoint.test"],
        port=contact.server_port,
        node_list_polling=NodeListPollingConfig(
            active_interval_ms=60_000,
            idle_interval_ms=60_000,
        ),
    )
    try:
        with (
            patch.dict(os.environ, {"NO_PROXY": "*", "no_proxy": "*"}),
            patch("socket.getaddrinfo", side_effect=resolve),
            patch("alternator.core.handlers.random.getrandbits", return_value=0),
            Helper(config, auth=Auth.disabled()) as helper,
        ):
            client = helper.client()
            assert client.list_tables()["TableNames"] == []
    finally:
        _stop_server(contact, contact_thread)
        _stop_server(good, good_thread)

    assert good_requests == ["DynamoDB_20120810.ListTables"]


@pytest.mark.asyncio
async def test_async_operations_continue_after_contact_host_recovery() -> None:
    """Async operations route to replacement IP learned from retained seed."""
    pytest.importorskip("aiobotocore")
    contact_nodes = ["127.0.0.2"]
    contact_hosts: list[str] = []
    old_requests: list[str] = []
    new_requests: list[str] = []
    contact, contact_thread = _start_server(
        "127.0.0.1",
        0,
        _contact_handler(contact_nodes, contact_hosts),
    )
    old, old_thread = _start_server(
        "127.0.0.2",
        contact.server_port,
        _dynamodb_handler(old_requests),
    )
    new, new_thread = _start_server(
        "127.0.0.3",
        contact.server_port,
        _dynamodb_handler(new_requests),
    )
    original_getaddrinfo = socket.getaddrinfo

    def resolve(
        host: bytes | str | None,
        port: int | str | None,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[AddressInfo]:
        if host == "entrypoint.test":
            assert isinstance(port, int)
            return _address_info("127.0.0.1", port)
        return cast(
            list[AddressInfo],
            original_getaddrinfo(host, port, family, type, proto, flags),
        )

    config = Config(
        seed_hosts=["entrypoint.test"],
        port=contact.server_port,
        node_list_polling=NodeListPollingConfig(
            active_interval_ms=60_000,
            idle_interval_ms=60_000,
        ),
    )
    try:
        with (
            patch.dict(os.environ, {"NO_PROXY": "*", "no_proxy": "*"}),
            patch("socket.getaddrinfo", side_effect=resolve),
        ):
            async with AsyncHelper(config, auth=Auth.disabled()) as helper:
                client = await helper.client()
                assert (await client.list_tables())["TableNames"] == []
                assert old_requests == ["DynamoDB_20120810.ListTables"]

                _stop_server(old, old_thread)
                contact_nodes[:] = ["127.0.0.3"]
                assert await helper.update_live_nodes() is True
                assert helper.get_nodes() == ["127.0.0.3"]

                assert (await client.list_tables())["TableNames"] == []
                assert new_requests == ["DynamoDB_20120810.ListTables"]
    finally:
        if old_thread.is_alive():
            _stop_server(old, old_thread)
        _stop_server(contact, contact_thread)
        _stop_server(new, new_thread)

    assert contact_hosts
    assert set(contact_hosts) == {f"entrypoint.test:{contact.server_port}"}


def test_sync_seed_fallback_precedes_scope_fallback() -> None:
    """Every seed is tried within a scope before moving to its fallback."""
    calls: list[str] = []
    config = Config(
        seed_hosts=["seed-a.test", "seed-b.test"],
        port=8000,
        routing_scope=DatacenterScope("wrong", fallback=ClusterScope()),
    )

    def fetch(url: str) -> Sequence[str]:
        calls.append(url)
        if "seed-b.test" in url and "?dc=" not in url:
            return ["127.0.0.9"]
        return []

    manager = SyncLiveNodesManager(config, fetch)

    assert manager.refresh_nodes() is True
    assert calls == [
        "http://seed-a.test:8000/localnodes?dc=wrong",
        "http://seed-b.test:8000/localnodes?dc=wrong",
        "http://seed-a.test:8000/localnodes",
        "http://seed-b.test:8000/localnodes",
    ]
    assert manager.nodes.scope_name == "Cluster"


@pytest.mark.asyncio
async def test_async_seed_fallback_precedes_scope_fallback() -> None:
    """Async discovery exhausts contact hosts before broader scope."""
    calls: list[str] = []
    config = Config(
        seed_hosts=["seed-a.test", "seed-b.test"],
        port=8000,
        routing_scope=DatacenterScope("wrong", fallback=ClusterScope()),
    )

    async def fetch(url: str) -> Sequence[str]:
        calls.append(url)
        if "seed-b.test" in url and "?dc=" not in url:
            return ["127.0.0.9"]
        return []

    manager = AsyncLiveNodesManager(config, fetch)

    assert await manager.refresh_nodes() is True
    assert calls == [
        "http://seed-a.test:8000/localnodes?dc=wrong",
        "http://seed-b.test:8000/localnodes?dc=wrong",
        "http://seed-a.test:8000/localnodes",
        "http://seed-b.test:8000/localnodes",
    ]
    assert manager.nodes.scope_name == "Cluster"
