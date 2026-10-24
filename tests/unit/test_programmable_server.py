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

"""Deterministic fake-peer behavior for transport and lifecycle contracts."""

from __future__ import annotations

import gzip
import http.client
import json
import shutil
import socket
import ssl
import subprocess
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from alternator import Config
from alternator._http import create_sync_http_fetcher
from alternator.core.live_nodes import SyncLiveNodesManager
from alternator.core.routing_scope import ClusterScope, DatacenterScope, RackScope
from tests.fake_server import FakeAlternatorResponse, FakeAlternatorServer


def post(
    connection: http.client.HTTPConnection, operation: str, body: object
) -> tuple[int, dict[str, object]]:
    connection.request(
        "POST",
        "/",
        json.dumps(body),
        {"X-Amz-Target": f"DynamoDB_20120810.{operation}"},
    )
    response = connection.getresponse()
    return response.status, json.loads(response.read())


def get(
    connection: http.client.HTTPConnection, path: str = "/localnodes"
) -> tuple[int, bytes]:
    connection.request("GET", path)
    response = connection.getresponse()
    return response.status, response.read()


def connect(server: FakeAlternatorServer) -> http.client.HTTPConnection:
    endpoint = urlsplit(server.url())
    assert endpoint.hostname is not None
    return http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=3)


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_response_header_overrides_are_case_insensitive(
    fake_alternator_server: FakeAlternatorServer, method: str
) -> None:
    headers = {
        "x-amzn-requestid": "custom-id",
        "content-TYPE": "custom/json",
        "X-Custom": "first",
        "x-custom": "last",
    }
    if method == "POST":
        fake_alternator_server.set_operation("GetItem", {}, headers=headers)
    else:
        fake_alternator_server.set_text("/", "response", headers=headers)
    with closing(connect(fake_alternator_server)) as connection:
        connection.request(
            method, "/", "{}", {"X-Amz-Target": "DynamoDB_20120810.GetItem"}
        )
        response = connection.getresponse()
        assert response.status == 200
        response.read()
        wire_headers = response.getheaders()
    for name, expected in (
        ("x-amzn-requestid", "custom-id"),
        ("content-TYPE", "custom/json"),
        ("x-custom", "last"),
    ):
        matches = [
            (key, value) for key, value in wire_headers if key.lower() == name.lower()
        ]
        assert matches == [(name, expected)]


def test_native_discovery_scripts_and_connection_reuse(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    server = fake_alternator_server
    server.set_topology(
        [("127.0.0.1", "dc1", "rack1")], [("127.0.0.2", "dc2", "rack2")]
    )
    server.set_partition_key("contract", "pk", key_type="B")
    server.script(
        "Scan",
        [
            FakeAlternatorResponse.json({"__type": "InternalServerError"}, status=500),
            FakeAlternatorResponse.json(
                {"Items": [], "LastEvaluatedKey": {"pk": {"S": "next"}}}
            ),
            FakeAlternatorResponse.json({"Items": []}),
        ],
        table="paginated",
    )
    connection = connect(server)
    try:
        assert get(connection) == (200, b'["127.0.0.1"]')
        assert get(connection, "/localnodes?dc=dc2") == (200, b'["127.0.0.2"]')
        _, body = post(connection, "DescribeTable", {"TableName": "contract"})
        assert body == {
            "Table": {
                "TableName": "contract",
                "TableStatus": "ACTIVE",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "B"}],
            }
        }
        assert post(connection, "Scan", {"TableName": "paginated"})[0] == 500
        _, body = post(connection, "Scan", {"TableName": "paginated"})
        assert post(
            connection,
            "Scan",
            {"TableName": "paginated", "ExclusiveStartKey": body["LastEvaluatedKey"]},
        ) == (200, {"Items": []})
        assert post(connection, "Scan", {"TableName": "paginated"})[0] == 400
    finally:
        connection.close()
    requests = server.captured_requests()
    assert len({r.connection_id for r in requests}) == 1
    assert requests[5].document["ExclusiveStartKey"] == {"pk": {"S": "next"}}
    assert all(request.method == "GET" for request in requests[:2])
    assert not any(
        request.table and request.table.startswith(".scylla.") for request in requests
    )


@pytest.mark.parametrize(
    "query,expected",
    [
        ("", ["127.0.0.1", "127.0.0.2"]),
        ("dc=dc%20one", ["127.0.0.1", "127.0.0.2"]),
        ("dc=dc%20one&rack=r%2F1", ["127.0.0.1"]),
        ("dc=dc2", ["127.0.0.3"]),
    ],
)
def test_topology_routes_respect_scopes(
    fake_alternator_server: FakeAlternatorServer, query: str, expected: list[str]
) -> None:
    server = fake_alternator_server
    server.set_topology(
        [("127.0.0.1", "dc one", "r/1")],
        [("127.0.0.2", "dc one", "r2"), ("127.0.0.3", "dc2", "r3")],
    )
    path = "/localnodes" + (f"?{query}" if query else "")
    with closing(connect(server)) as connection:
        status, body = get(connection, path)
        assert status == 200
        assert json.loads(body) == expected
        assert get(connection, "/localnodes?dc=unknown")[0] == 404
        assert (
            post(connection, "Scan", {"TableName": ".scylla.alternator.system.local"})[
                0
            ]
            == 400
        )


@pytest.mark.parametrize(
    "query",
    [
        "dc=dc%20one%2B%25%26",
        "dc=dc+one%2B%25%26",
        "dc=dc%20one%2B%25%26&rack=r%20%2F%2B%25%26",
        "dc=dc+one%2B%25%26&rack=r+%2F%2B%25%26",
    ],
    ids=["python-dc", "rust-dc", "python-rack", "rust-rack"],
)
def test_topology_supports_python_and_rust_query_encodings(
    fake_alternator_server: FakeAlternatorServer, query: str
) -> None:
    server = fake_alternator_server
    server.set_topology(
        [("127.0.0.1", "dc one+%&", "r /+%&")],
        [("127.0.0.2", "dc other", "r2")],
    )
    with closing(connect(server)) as connection:
        assert get(connection, f"/localnodes?{query}") == (200, b'["127.0.0.1"]')
        # Form-encoded spaces must not reinterpret a literal '+' as whitespace.
        assert get(connection, "/localnodes?dc=dc+one+%25%26")[0] == 404


@pytest.mark.parametrize(
    "query",
    [
        "dc=dc%20~%2A%2B%25%26",
        "dc=dc+%7E*%2B%25%26",
        "dc=dc%20~%2A%2B%25%26&rack=r%20~%2A%2B%25%26%2F",
        "dc=dc+%7E*%2B%25%26&rack=r+%7E*%2B%25%26%2F",
    ],
    ids=["python-dc", "rust-dc", "python-rack", "rust-rack"],
)
def test_topology_supports_form_encoded_tilde_and_asterisk(
    fake_alternator_server: FakeAlternatorServer, query: str
) -> None:
    server = fake_alternator_server
    server.set_topology([("127.0.0.1", "dc ~*+%&", "r ~*+%&/")])
    with closing(connect(server)) as connection:
        assert get(connection, f"/localnodes?{query}") == (200, b'["127.0.0.1"]')


@pytest.mark.parametrize(
    "python_value,rust_value", [("dc%20one", "dc+one"), ("~%2A", "%7E*")]
)
def test_get_scripts_keep_encodings_distinct(
    fake_alternator_server: FakeAlternatorServer, python_value: str, rust_value: str
) -> None:
    server = fake_alternator_server
    server.script_get(
        f"/localnodes?dc={python_value}", [FakeAlternatorResponse.json([])]
    )
    with closing(connect(server)) as connection:
        assert get(connection, f"/localnodes?dc={rust_value}")[0] == 404
        assert get(connection, f"/localnodes?dc={python_value}") == (200, b"[]")


def test_get_scripts_preserve_raw_failures_and_exhaustion(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    server = fake_alternator_server
    responses = [
        FakeAlternatorResponse(200, b"not JSON", {"Content-Type": "text/plain"}),
        FakeAlternatorResponse.json([]),
        FakeAlternatorResponse.json({"message": "retry"}, status=503),
    ]
    server.script_get("/localnodes", responses)
    server.set_localnodes(["127.0.0.2"], query="dc=other")
    with closing(connect(server)) as connection:
        for response in responses:
            assert get(connection) == (response.status, response.body)
        assert get(connection)[0] == 404
        assert get(connection, "/localnodes?dc=other") == (200, b'["127.0.0.2"]')
    requests = server.wait_for_requests(4, path="/localnodes")
    assert len(requests) == 4
    assert all(
        request.method == "GET" and request.operation == "" for request in requests
    )
    assert len({request.connection_id for request in server.captured_requests()}) == 1


@pytest.mark.parametrize("failure", ["malformed", "empty", "error"])
@pytest.mark.parametrize("fallback", [False, True], ids=["strict", "fallback"])
def test_real_python_discovery_preserves_explicit_fallbacks(
    fake_alternator_server: FakeAlternatorServer, failure: str, fallback: bool
) -> None:
    server = fake_alternator_server
    rack_path = "/localnodes?dc=dc1&rack=r1"
    response = {
        "malformed": FakeAlternatorResponse(200, b"not JSON", {}),
        "empty": FakeAlternatorResponse.json([]),
        "error": FakeAlternatorResponse.json({"message": "unavailable"}, status=503),
    }[failure]
    server.script_get(rack_path, [], fallback=response)
    server.set_localnodes(["127.0.0.1"], query="dc=dc1")
    server.set_localnodes(["127.0.0.2"])
    scope = RackScope(
        "dc1",
        "r1",
        fallback=DatacenterScope("dc1", fallback=ClusterScope()) if fallback else None,
    )
    manager = SyncLiveNodesManager(
        Config(["127.0.0.1"], urlsplit(server.url()).port or 0, routing_scope=scope),
        create_sync_http_fetcher(timeout_seconds=1),
    )
    assert manager.refresh_nodes() is fallback
    expected_paths = [rack_path, "/localnodes?dc=dc1"] if fallback else [rack_path]
    assert server.requested_paths() == expected_paths
    assert all(request.method == "GET" for request in server.captured_requests())


def test_get_script_fallback_and_disconnection(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    server = fake_alternator_server
    server.script_get(
        "/localnodes",
        [FakeAlternatorResponse(200, b"", {}, disconnect=True)],
        fallback=FakeAlternatorResponse.json(["127.0.0.1"]),
    )
    with (
        closing(connect(server)) as connection,
        pytest.raises(http.client.RemoteDisconnected),
    ):
        get(connection)
    with closing(connect(server)) as connection:
        assert get(connection) == (200, b'["127.0.0.1"]')
        assert get(connection) == (200, b'["127.0.0.1"]')
    requests = server.wait_for_requests(3, path="/localnodes")
    assert requests[0].connection_id != requests[1].connection_id
    assert requests[1].connection_id == requests[2].connection_id


@pytest.mark.parametrize("held_method", ["GET", "POST"])
def test_blocked_request_allows_parallel_progress(
    fake_alternator_server: FakeAlternatorServer, held_method: str
) -> None:
    entered, release = threading.Event(), threading.Event()
    server = fake_alternator_server
    held = FakeAlternatorResponse.json(
        {"Item": {}} if held_method == "POST" else ["127.0.0.1"],
        entered=entered,
        release=release,
    )
    if held_method == "POST":
        server.script("GetItem", [held])
    else:
        server.script_get("/localnodes", [held])
    server.set_operation("PutItem", {})
    first, second = connect(server), connect(server)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = (
                pool.submit(post, first, "GetItem", {"TableName": "contract"})
                if held_method == "POST"
                else pool.submit(get, first)
            )
            try:
                assert entered.wait(2)
                assert post(second, "PutItem", {"TableName": "contract"}) == (200, {})
                requests = server.wait_for_requests(2)
                assert not pending.done()
                assert len({r.connection_id for r in requests}) == 2
            finally:
                release.set()
            assert pending.result(timeout=2) == (
                (200, {"Item": {}})
                if held_method == "POST"
                else (200, b'["127.0.0.1"]')
            )
    finally:
        first.close()
        second.close()


def test_gzip_capture_preserves_wire_bytes(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    server = fake_alternator_server
    server.set_operation("PutItem", {})
    connection = connect(server)
    compressed = gzip.compress(b'{"TableName":"contract","Item":{"pk":{"B":"AP8="}}}')
    try:
        connection.request(
            "POST",
            "/",
            compressed,
            {
                "X-Amz-Target": "DynamoDB_20120810.PutItem",
                "Content-Encoding": "gzip",
                "X-Custom-Case": "original",
            },
        )
        response = connection.getresponse()
        assert response.status == 200
        response.read()
    finally:
        connection.close()
    request = server.wait_for_requests(1)[0]
    assert request.body == compressed
    assert request.document["Item"] == {"pk": {"B": "AP8="}}
    assert request.header("x-custom-case") == "original"
    assert ("X-Custom-Case", "original") in request.headers


@pytest.mark.parametrize("held_method", ["GET", "POST"])
def test_stop_interrupts_held_response(held_method: str) -> None:
    server = FakeAlternatorServer()
    server.start()
    entered = threading.Event()
    held = FakeAlternatorResponse.json({}, entered=entered, release=threading.Event())
    if held_method == "POST":
        server.script("GetItem", [held])
    else:
        server.script_get("/localnodes", [held])
    connection = connect(server)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = (
                pool.submit(post, connection, "GetItem", {})
                if held_method == "POST"
                else pool.submit(get, connection)
            )
            assert entered.wait(2)
            server.stop()
            with pytest.raises(OSError):
                pending.result(timeout=2)
    finally:
        connection.close()
        server.stop()


@pytest.fixture
def tls_material(tmp_path: Path) -> tuple[Path, Path]:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("TLS peer tests require openssl certificate generation")
    cert, key = tmp_path / "server.crt", tmp_path / "server.key"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-addext",
            "extendedKeyUsage=serverAuth,clientAuth",
        ],
        check=True,
        capture_output=True,
        timeout=10,
    )
    return cert, key


@pytest.fixture
def tls_server(tls_material: tuple[Path, Path]) -> Iterator[FakeAlternatorServer]:
    cert, key = tls_material
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    server = FakeAlternatorServer()
    server.set_operation("GetItem", {})
    server.set_localnodes(["127.0.0.1"])
    server.start(ssl_context=context)
    try:
        yield server
    finally:
        server.stop()


def test_verified_tls_and_hostname(
    tls_server: FakeAlternatorServer, tls_material: tuple[Path, Path]
) -> None:
    cert, _ = tls_material
    port = urlsplit(tls_server.url()).port
    trusted = ssl.create_default_context(cafile=str(cert))
    with closing(
        http.client.HTTPSConnection("localhost", port, context=trusted, timeout=2)
    ) as connection:
        assert post(connection, "GetItem", {}) == (200, {})
        assert get(connection) == (200, b'["127.0.0.1"]')
    with (
        pytest.raises(ssl.SSLCertVerificationError),
        closing(
            http.client.HTTPSConnection("127.0.0.1", port, context=trusted, timeout=2)
        ) as connection,
    ):
        post(connection, "GetItem", {})
    with (
        pytest.raises(ssl.SSLCertVerificationError),
        closing(
            http.client.HTTPSConnection(
                "localhost", port, context=ssl.create_default_context(), timeout=2
            )
        ) as connection,
    ):
        post(connection, "GetItem", {})
    assert len(tls_server.captured_requests()) == 2


def test_mutual_tls_requires_client_certificate(
    tls_material: tuple[Path, Path],
) -> None:
    cert, key = tls_material
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    context.load_verify_locations(cafile=str(cert))
    context.verify_mode = ssl.CERT_REQUIRED
    server = FakeAlternatorServer()
    server.set_operation("GetItem", {})
    server.set_localnodes(["127.0.0.1"])
    server.start(ssl_context=context)
    port = urlsplit(server.url()).port
    trusted = ssl.create_default_context(cafile=str(cert))
    try:
        with (
            pytest.raises(OSError),
            closing(
                http.client.HTTPSConnection(
                    "localhost", port, context=trusted, timeout=2
                )
            ) as connection,
        ):
            post(connection, "GetItem", {})
        trusted.load_cert_chain(cert, key)
        with closing(
            http.client.HTTPSConnection("localhost", port, context=trusted, timeout=2)
        ) as connection:
            assert post(connection, "GetItem", {}) == (200, {})
        assert len(server.captured_requests()) == 1
    finally:
        server.stop()


def test_pending_tls_handshake_allows_other_clients(
    tls_server: FakeAlternatorServer, tls_material: tuple[Path, Path]
) -> None:
    cert, _ = tls_material
    port = urlsplit(tls_server.url()).port
    assert port is not None
    # Leave this TCP peer open without sending a TLS ClientHello.
    with closing(socket.create_connection(("127.0.0.1", port), timeout=2)):
        tls_server.wait_for_connections(1, timeout=2)
        trusted = ssl.create_default_context(cafile=str(cert))
        with closing(
            http.client.HTTPSConnection("localhost", port, context=trusted, timeout=2)
        ) as connection:
            assert post(connection, "GetItem", {}) == (200, {})
    assert len(tls_server.captured_requests()) == 1


def test_stop_interrupts_pending_tls_handshake(
    tls_server: FakeAlternatorServer,
) -> None:
    port = urlsplit(tls_server.url()).port
    assert port is not None
    peer = socket.create_connection(("127.0.0.1", port), timeout=2)
    stopped = threading.Event()
    errors: list[BaseException] = []

    def stop() -> None:
        try:
            tls_server.stop()
        except BaseException as error:
            errors.append(error)
        finally:
            stopped.set()

    stopper = threading.Thread(target=stop, daemon=True)
    try:
        tls_server.wait_for_connections(1, timeout=2)
        stopper.start()
        assert stopped.wait(2), "stop must interrupt a peer withholding ClientHello"
        assert not errors
    finally:
        # Release the peer even if the regression assertion fails, so fixture
        # cleanup and the stopper thread cannot remain stuck in a handshake.
        peer.close()
        if stopper.ident is not None:
            stopper.join(timeout=3)
            assert not stopper.is_alive()
