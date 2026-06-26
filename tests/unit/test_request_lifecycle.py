"""Tests for Alternator request mutation timing."""

from __future__ import annotations

from typing import Any

import boto3
import pytest
from botocore.awsrequest import AWSPreparedRequest, AWSRequest
from botocore.config import Config as BotoConfig

from alternator.config import CompressionAlgorithm, Config, RequestCompressionConfig
from alternator.core.handlers import _register_alternator_handlers
from alternator.core.live_nodes import NodeList


class _StaticManager:
    def __init__(self, nodes: tuple[str, ...]) -> None:
        self._nodes = NodeList(nodes=nodes, scope_name="cluster")

    @property
    def nodes(self) -> NodeList:
        return self._nodes


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
