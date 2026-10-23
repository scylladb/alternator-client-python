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
    local = ".scylla.alternator.system.local"
    peers = ".scylla.alternator.system.peers"
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
        _, body = post(
            connection,
            "Scan",
            {
                "TableName": local,
                "ProjectionExpression": "rpc_address,data_center,rack",
            },
        )
        assert body["Items"] == [
            {
                "rpc_address": {"S": "127.0.0.1"},
                "data_center": {"S": "dc1"},
                "rack": {"S": "rack1"},
            }
        ]
        _, body = post(connection, "Scan", {"TableName": peers})
        assert body["Count"] == 1
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


def test_blocked_request_allows_parallel_progress(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    entered, release = threading.Event(), threading.Event()
    server = fake_alternator_server
    server.script(
        "GetItem",
        [FakeAlternatorResponse.json({"Item": {}}, entered=entered, release=release)],
    )
    server.set_operation("PutItem", {})
    first, second = connect(server), connect(server)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(post, first, "GetItem", {"TableName": "contract"})
            try:
                assert entered.wait(2)
                assert post(second, "PutItem", {"TableName": "contract"}) == (200, {})
                requests = server.wait_for_requests(2)
                assert not pending.done()
                assert len({r.connection_id for r in requests}) == 2
            finally:
                release.set()
            assert pending.result(timeout=2) == (200, {"Item": {}})
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


def test_stop_interrupts_held_response() -> None:
    server = FakeAlternatorServer()
    server.start()
    entered = threading.Event()
    server.script(
        "GetItem",
        [FakeAlternatorResponse.json({}, entered=entered, release=threading.Event())],
    )
    connection = connect(server)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(post, connection, "GetItem", {})
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
    assert len(tls_server.captured_requests()) == 1


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
