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

"""Integration tests for vector search support.

These tests require a running ScyllaDB cluster with Alternator and vector
search support enabled.  Start a local cluster with: make scylla-start

Vector search is a ScyllaDB Alternator extension; all tests in this file are
skipped when running against a Scylla version that predates the feature.
"""

import uuid
from collections.abc import Callable
from decimal import Decimal

import pytest

from alternator import (
    AlternatorClient,
    AlternatorResource,
    Config,
)
from alternator.vector import Vector
from tests.integration import (
    SCYLLA_HOST,
    SCYLLA_PORT,
    SKIP_INTEGRATION,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


config = Config(seed_hosts=[SCYLLA_HOST], port=SCYLLA_PORT, scheme="http")


def table_name() -> str:
    return f"test_vec_{uuid.uuid4().hex[:8]}"


def test_create_table_with_vector_index(
    skip_if_scylla_version_below: Callable[..., None],
) -> None:
    """CreateTable with VectorIndexes should succeed and DescribeTable should
    return the index."""
    from tests.integration.scylla_version import ScyllaVersion

    skip_if_scylla_version_below(ScyllaVersion(2026, 2, 0), "vector search")

    name = table_name()
    with AlternatorClient(config) as client:
        client.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            VectorIndexes=[
                {
                    "IndexName": "embedding_index",
                    "VectorAttribute": {"AttributeName": "embedding", "Dimensions": 4},
                    "SimilarityFunction": "COSINE",
                }
            ],
        )
        waiter = client.get_waiter("table_exists")
        waiter.wait(TableName=name)

        try:
            desc = client.describe_table(TableName=name)
            indexes = desc["Table"].get("VectorIndexes", [])
            assert any(idx["IndexName"] == "embedding_index" for idx in indexes), (
                f"Expected 'embedding_index' in VectorIndexes, got: {indexes}"
            )
        finally:
            client.delete_table(TableName=name)


def test_create_table_with_vector_index_via_resource(
    skip_if_scylla_version_below: Callable[..., None],
) -> None:
    """Vector index creation should work through the high-level Resource
    interface as well as the low-level client."""
    from tests.integration.scylla_version import ScyllaVersion

    skip_if_scylla_version_below(ScyllaVersion(2026, 2, 0), "vector search")

    name = table_name()
    with AlternatorResource(config) as resource:
        resource.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            VectorIndexes=[
                {
                    "IndexName": "embedding_index",
                    "VectorAttribute": {"AttributeName": "embedding", "Dimensions": 4},
                }
            ],
        )
        table = resource.Table(name)
        table.wait_until_exists()

        try:
            desc = resource.meta.client.describe_table(TableName=name)
            indexes = desc["Table"].get("VectorIndexes", [])
            assert any(idx["IndexName"] == "embedding_index" for idx in indexes)
        finally:
            table.delete()


def test_vector_roundtrip_via_resource(
    skip_if_scylla_version_below: Callable[..., None],
) -> None:
    """A Vector stored via put_item should be returned as a Vector by
    get_item, and its values should be close to the original floats
    (within 32-bit float precision)."""
    from tests.integration.scylla_version import ScyllaVersion

    skip_if_scylla_version_below(ScyllaVersion(2026, 2, 0), "vector search")

    name = table_name()
    with AlternatorResource(config) as resource:
        resource.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table = resource.Table(name)
        table.wait_until_exists()

        try:
            original = Vector([0.1, 0.2, 0.3, 0.4])
            table.put_item(Item={"pk": "item1", "embedding": original})

            returned = table.get_item(Key={"pk": "item1"}, ConsistentRead=True)["Item"][
                "embedding"
            ]

            # Should come back as a Vector instance
            assert isinstance(returned, Vector), (
                f"Expected Vector, got {type(returned)}"
            )
            # Values should be close to the originals (32-bit float precision)
            assert len(returned) == len(original)
            for got, expected in zip(returned, original, strict=True):
                assert abs(float(got) - expected) < 1e-6, (
                    f"Value mismatch: got {got}, expected {expected}"
                )
        finally:
            table.delete()


def test_vector_differs_from_decimal_list(
    skip_if_scylla_version_below: Callable[..., None],
) -> None:
    """A plain list of Decimals and a Vector containing the same values
    should both round-trip, but the Vector comes back as a Vector instance
    while the plain list comes back as a list of Decimals — confirming that
    the two wire types are distinct."""
    from tests.integration.scylla_version import ScyllaVersion

    skip_if_scylla_version_below(ScyllaVersion(2026, 2, 0), "vector search")

    name = table_name()
    with AlternatorResource(config) as resource:
        resource.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table = resource.Table(name)
        table.wait_until_exists()

        try:
            table.put_item(
                Item={
                    "pk": "dec",
                    "v": [Decimal("0.1"), Decimal("0.2"), Decimal("0.3")],
                }
            )
            table.put_item(Item={"pk": "vec", "v": Vector([0.1, 0.2, 0.3])})

            dec_returned = table.get_item(Key={"pk": "dec"}, ConsistentRead=True)[
                "Item"
            ]["v"]
            vec_returned = table.get_item(Key={"pk": "vec"}, ConsistentRead=True)[
                "Item"
            ]["v"]

            # Decimal list comes back as a plain list of Decimals
            assert not isinstance(dec_returned, Vector)
            assert all(isinstance(x, Decimal) for x in dec_returned)

            # Vector comes back as a Vector
            assert isinstance(vec_returned, Vector)
        finally:
            table.delete()
