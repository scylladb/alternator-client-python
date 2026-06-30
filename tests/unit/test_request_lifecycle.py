"""Tests for Alternator request mutation timing."""

from __future__ import annotations

import json
import socket
from collections.abc import Sequence
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any, cast

import boto3
import pytest
from botocore import UNSIGNED
from botocore.awsrequest import AWSPreparedRequest, AWSRequest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, EndpointConnectionError

from alternator.config import (
    DEFAULT_USER_AGENT,
    CompressionAlgorithm,
    Config,
    HeaderOptimizationConfig,
    RequestCompressionConfig,
)
from alternator.core.handlers import _register_alternator_handlers
from alternator.core.live_nodes import NodeList


class _StaticManager:
    def __init__(self, nodes: tuple[str, ...]) -> None:
        self._nodes = NodeList(nodes=nodes, scope_name="cluster")

    @property
    def nodes(self) -> NodeList:
        return self._nodes


class _CountingHTTPServer(HTTPServer):
    """HTTP server that counts accepted TCP connections."""

    connection_count: int

    def __init__(
        self,
        server_address: tuple[str, int],
        request_handler_class: type[BaseHTTPRequestHandler],
    ) -> None:
        self.connection_count = 0
        super().__init__(server_address, request_handler_class)

    def get_request(self) -> tuple[socket.socket, object]:
        request, client_address = super().get_request()
        self.connection_count += 1
        return request, client_address


class _SequencedDynamoDBHandler(BaseHTTPRequestHandler):
    """Serve configured DynamoDB responses over one keep-alive connection."""

    protocol_version = "HTTP/1.1"
    configured_responses: Sequence[tuple[int, bytes]] = ()
    request_count = 0

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        if content_length:
            self.rfile.read(content_length)

        index = type(self).request_count
        type(self).request_count += 1
        status, body = type(self).configured_responses[index]
        keep_alive = index + 1 < len(type(self).configured_responses)

        self.send_response(status)
        self.send_header("Content-Type", "application/x-amz-json-1.0")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "keep-alive" if keep_alive else "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def _make_dynamodb_handler(
    responses: Sequence[tuple[int, bytes]],
) -> type[_SequencedDynamoDBHandler]:
    """Create an isolated DynamoDB handler class."""
    local_responses = tuple(responses)

    class Handler(_SequencedDynamoDBHandler):
        configured_responses = local_responses
        request_count = 0

    return Handler


def _header_text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def test_signed_request_url_and_compressed_body_are_final_before_signing() -> None:
    """Endpoint and body mutations happen before SigV4 signing."""
    config = Config(
        seed_hosts=["seed"],
        port=8000,
        request_compression=RequestCompressionConfig(
            algorithm=CompressionAlgorithm.GZIP,
            min_size_bytes=10,
        ),
    )
    client = boto3.client(
        "dynamodb",
        endpoint_url="http://seed:8000",
        region_name="us-east-1",
        aws_access_key_id="alternator",
        aws_secret_access_key="secret",
        config=BotoConfig(retries={"max_attempts": 0, "mode": "standard"}),
    )
    _register_alternator_handlers(
        client.meta.events,
        _StaticManager(("node-b",)),
        config,
        auth_enabled=True,
    )
    seen: dict[str, Any] = {}

    def capture_before_sign(request: AWSRequest, **_: object) -> None:
        seen["before_sign_url"] = request.url
        seen["before_sign_body"] = request.body
        seen["before_sign_headers"] = dict(request.headers)

    def capture_before_send(request: AWSPreparedRequest, **_: object) -> None:
        seen["before_send_url"] = request.url
        seen["before_send_body"] = request.body
        seen["authorization"] = request.headers.get("Authorization")
        raise RuntimeError("captured")

    client.meta.events.register("before-sign.dynamodb.PutItem", capture_before_sign)
    client.meta.events.register_last(
        "before-send.dynamodb.PutItem", capture_before_send
    )

    with pytest.raises(RuntimeError, match="captured"):
        client.put_item(
            TableName="tbl",
            Item={"pk": {"S": "k"}, "data": {"S": "x" * 5000}},
        )

    assert seen["before_sign_url"] == "http://node-b:8000/"
    assert seen["before_send_url"] == seen["before_sign_url"]
    assert seen["before_sign_body"] == seen["before_send_body"]
    assert seen["before_sign_body"].startswith(b"\x1f\x8b")
    assert seen["before_sign_headers"]["Content-Encoding"] == "gzip"
    assert seen["authorization"] is not None


def test_unset_user_agent_removes_sdk_user_agent_before_send() -> None:
    """Final request does not include User-Agent when Alternator value is unset."""
    config = Config(
        seed_hosts=["seed"],
        port=8000,
        header_optimization=HeaderOptimizationConfig(enabled=True),
    )
    client = boto3.client(
        "dynamodb",
        endpoint_url="http://seed:8000",
        region_name="us-east-1",
        config=BotoConfig(
            signature_version=UNSIGNED,
            user_agent="Boto3/1.0 Botocore/1.0",
        ),
    )
    _register_alternator_handlers(
        client.meta.events,
        _StaticManager(("node-b",)),
        config,
        auth_enabled=False,
        user_agent=None,
    )
    seen: dict[str, object] = {}

    def capture_before_send(request: AWSPreparedRequest, **_: object) -> None:
        seen["user_agent"] = request.headers.get("User-Agent")
        raise RuntimeError("captured")

    client.meta.events.register_last(
        "before-send.dynamodb.ListTables", capture_before_send
    )

    with pytest.raises(RuntimeError, match="captured"):
        client.list_tables()

    assert seen["user_agent"] is None


def test_configured_user_agent_replaces_sdk_user_agent_before_send() -> None:
    """Final User-Agent contains only the configured Alternator value."""
    config = Config(
        seed_hosts=["seed"],
        port=8000,
        header_optimization=HeaderOptimizationConfig(enabled=True),
        user_agent=DEFAULT_USER_AGENT,
    )
    client = boto3.client(
        "dynamodb",
        endpoint_url="http://seed:8000",
        region_name="us-east-1",
        config=BotoConfig(
            signature_version=UNSIGNED,
            user_agent="Boto3/1.0 Botocore/1.0",
        ),
    )
    _register_alternator_handlers(
        client.meta.events,
        _StaticManager(("node-b",)),
        config,
        auth_enabled=False,
        user_agent=DEFAULT_USER_AGENT,
    )
    seen: dict[str, str] = {}

    def capture_before_send(request: AWSPreparedRequest, **_: object) -> None:
        seen["user_agent"] = _header_text(request.headers.get("User-Agent"))
        raise RuntimeError("captured")

    client.meta.events.register_last(
        "before-send.dynamodb.ListTables", capture_before_send
    )

    with pytest.raises(RuntimeError, match="captured"):
        client.list_tables()

    assert seen["user_agent"] == DEFAULT_USER_AGENT
    assert "Boto3" not in seen["user_agent"]
    assert "Botocore" not in seen["user_agent"]


def test_sdk_retries_advance_shared_query_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retries for one SDK call walk the node plan across fresh requests."""
    config = Config(seed_hosts=["seed"], port=8000)
    client = boto3.client(
        "dynamodb",
        endpoint_url="http://seed:8000",
        region_name="us-east-1",
        config=BotoConfig(
            signature_version=UNSIGNED,
            retries={"max_attempts": 2, "mode": "standard"},
        ),
    )

    def preferred_node(
        operation_name: str,
        params: dict[str, Any],
        nodes: NodeList,
    ) -> str | None:
        assert operation_name == "PutItem"
        assert nodes.nodes == ("node-a", "node-b", "node-c")
        return "node-b"

    _register_alternator_handlers(
        client.meta.events,
        _StaticManager(("node-a", "node-b", "node-c")),
        config,
        preferred_node,
        auth_enabled=False,
    )
    urls: list[str] = []

    def capture_before_send(request: AWSPreparedRequest, **_: object) -> None:
        urls.append(request.url)

    def fail_send(request: AWSPreparedRequest) -> None:
        raise EndpointConnectionError(endpoint_url=request.url)

    def retry_without_sleep(attempts: int, **_: object) -> int | None:
        return 0 if attempts < 3 else None

    client.meta.events.register_last(
        "before-send.dynamodb.PutItem", capture_before_send
    )
    client.meta.events.register_first(
        "needs-retry.dynamodb.PutItem",
        cast(Any, retry_without_sleep),
    )
    monkeypatch.setattr(cast(Any, client)._endpoint.http_session, "send", fail_send)

    with pytest.raises(EndpointConnectionError):
        client.put_item(TableName="tbl", Item={"pk": {"S": "k"}})

    assert urls[0] == "http://node-b:8000/"
    assert set(urls[1:]) == {"http://node-a:8000/", "http://node-c:8000/"}


def test_dynamodb_non_success_responses_keep_connection_reusable() -> None:
    """DynamoDB responses are consumed so repeated non-2xx does not kill reuse."""
    handler = _make_dynamodb_handler(
        (
            (
                400,
                json.dumps({"__type": "ValidationException", "message": "bad"}).encode(
                    "utf-8"
                ),
            ),
            (
                400,
                json.dumps(
                    {"__type": "ValidationException", "message": "bad again"}
                ).encode("utf-8"),
            ),
            (200, json.dumps({"TableNames": []}).encode("utf-8")),
        )
    )
    server = _CountingHTTPServer(("127.0.0.1", 0), handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    client = boto3.client(
        "dynamodb",
        endpoint_url=f"http://seed:{server.server_port}",
        region_name="us-east-1",
        config=BotoConfig(
            signature_version=UNSIGNED,
            max_pool_connections=1,
            retries={"max_attempts": 0, "mode": "standard"},
        ),
    )
    _register_alternator_handlers(
        client.meta.events,
        _StaticManager(("127.0.0.1",)),
        Config(seed_hosts=["seed"], port=server.server_port),
        auth_enabled=False,
    )

    try:
        with pytest.raises(ClientError):
            client.list_tables()
        with pytest.raises(ClientError):
            client.list_tables()
        assert client.list_tables()["TableNames"] == []
    finally:
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)

    assert handler.request_count == 3
    assert server.connection_count == 1
