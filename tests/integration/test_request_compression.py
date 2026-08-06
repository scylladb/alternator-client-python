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

"""Integration tests for request compression on prepared requests."""

from __future__ import annotations

import gzip
import uuid
from collections.abc import Callable

import pytest
from botocore.exceptions import ClientError

from alternator import (
    AlternatorConfigBuilder,
    Auth,
    CompressionAlgorithm,
    close_client,
    create_client,
)
from alternator.async_client import close_async_client, create_async_client
from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION
from tests.integration.wire_capture import (
    capture_prepared_requests,
    inject_custom_headers,
    latest_request,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


def _large_item_payload() -> str:
    return "wire-compression-payload-" * 500


def _gzip_config(min_size: int = 100) -> object:
    return (
        AlternatorConfigBuilder()
        .with_seeds(SCYLLA_HOST)
        .with_port(SCYLLA_PORT)
        .with_compression(CompressionAlgorithm.GZIP, min_size=min_size)
        .build()
    )


class TestRequestCompression:
    """Verify sync request compression behavior on the wire."""

    def test_small_payload_is_not_gzipped_on_wire(self) -> None:
        client = create_client(_gzip_config(min_size=10_000))
        try:
            captured = capture_prepared_requests(client)
            client.list_tables()
            request = latest_request(captured)
            assert request.header("content-encoding") is None
        finally:
            close_client(client)

    def test_large_payload_is_gzipped_on_wire(
        self,
        skip_if_scylla_version_below: Callable[..., None],
    ) -> None:
        from tests.integration.scylla_version import ScyllaVersion

        skip_if_scylla_version_below(
            ScyllaVersion(2026, 1, 0), "gzip request compression"
        )

        client = create_client(_gzip_config())
        try:
            captured = capture_prepared_requests(client)
            with pytest.raises(ClientError):
                client.put_item(
                    TableName=f"missing_compression_{uuid.uuid4().hex}",
                    Item={
                        "pk": {"S": "compression-test"},
                        "data": {"S": _large_item_payload()},
                    },
                )

            request = latest_request(captured)
            assert request.header("content-encoding") == "gzip"
            assert b"wire-compression-payload" in gzip.decompress(request.body)
        finally:
            close_client(client)

    def test_headers_and_compression_share_wire_whitelist(
        self,
        skip_if_scylla_version_below: Callable[..., None],
    ) -> None:
        from tests.integration.scylla_version import ScyllaVersion

        skip_if_scylla_version_below(
            ScyllaVersion(2026, 1, 0), "gzip request compression"
        )

        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .with_compression(CompressionAlgorithm.GZIP, min_size=100)
            .with_header_optimization(whitelist={"X-Keep-Me"})
            .build()
        )
        client = create_client(
            config,
            auth=Auth.static_credentials("alternator", "secret"),
        )
        try:
            inject_custom_headers(client)
            captured = capture_prepared_requests(client)
            with pytest.raises(ClientError):
                client.put_item(
                    TableName=f"missing_compression_headers_{uuid.uuid4().hex}",
                    Item={
                        "pk": {"S": "compression-header-test"},
                        "data": {"S": _large_item_payload()},
                    },
                )

            request = latest_request(captured)
            assert request.header("content-encoding") == "gzip"
            assert request.header("content-length") is not None
            assert request.header("x-keep-me") == "keep"
            assert request.header("x-drop-me") is None
            assert request.header("authorization") is not None
        finally:
            close_client(client)


class TestAsyncRequestCompression:
    """Verify async request compression behavior on the wire."""

    @pytest.mark.asyncio
    async def test_large_payload_is_gzipped_on_wire(
        self,
        skip_if_scylla_version_below: Callable[..., None],
    ) -> None:
        from tests.integration.scylla_version import ScyllaVersion

        skip_if_scylla_version_below(
            ScyllaVersion(2026, 1, 0), "gzip request compression"
        )

        client = await create_async_client(_gzip_config())
        try:
            captured = capture_prepared_requests(client)
            with pytest.raises(ClientError):
                await client.put_item(
                    TableName=f"missing_async_compression_{uuid.uuid4().hex}",
                    Item={
                        "pk": {"S": "async-compression-test"},
                        "data": {"S": _large_item_payload()},
                    },
                )

            request = latest_request(captured)
            assert request.header("content-encoding") == "gzip"
            assert b"wire-compression-payload" in gzip.decompress(request.body)
        finally:
            await close_async_client(client)
