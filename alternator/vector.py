"""Vector search support for ScyllaDB Alternator.

This module provides the :class:`Vector` type and the
:func:`enable_vector_support` function that add ScyllaDB Alternator's
vector search extensions to a boto3 DynamoDB client or resource.

Vector search is a ScyllaDB Alternator extension that does not exist in the
standard AWS DynamoDB API.  Because boto3 / botocore is generated from the
official DynamoDB service description, it knows nothing about the new
operations and new wire types introduced by Alternator.  This module patches
botocore's in-memory service model at runtime to teach it about:

* ``VectorIndexes`` in ``CreateTable`` and ``DescribeTable``
* ``VectorIndexUpdates`` in ``UpdateTable``
* ``VectorSearch`` parameter and ``Scores`` response field in ``Query``
* The ``FLOAT32VECTOR`` attribute type in ``AttributeValue``

It also patches :class:`boto3.dynamodb.types.TypeSerializer` /
:class:`boto3.dynamodb.types.TypeDeserializer` so that the high-level
DynamoDB Resource interface automatically converts :class:`Vector` instances
to the ``FLOAT32VECTOR`` wire format and back.

:func:`enable_vector_support` is called automatically by
:func:`~alternator.client`, :func:`~alternator.resource`, and
:class:`~alternator.async_client.AsyncSession`, so users of the
Alternator client library do not need to call it manually.

Usage::

    import alternator

    config = alternator.Config(seed_hosts=["192.168.1.1"], port=8000)
    with alternator.client("dynamodb", cluster_config=config) as client:
        # vector support enabled automatically

        # Create a table with a vector index
        client.create_table(
            TableName="embeddings",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            VectorIndexes=[{
                "IndexName": "embedding_index",
                "VectorAttribute": {"AttributeName": "embedding", "Dimensions": 4},
                "SimilarityFunction": "COSINE",
            }],
        )

        # Store a vector using the low-level client interface
        client.put_item(
            TableName="embeddings",
            Item={"id": {"S": "item1"}, "embedding": {"FLOAT32VECTOR": [0.1, 0.2, 0.3, 0.4]}},
        )

        # Query by vector similarity
        result = client.query(
            TableName="embeddings",
            VectorSearch={"QueryVector": {"FLOAT32VECTOR": [0.1, 0.2, 0.3, 0.4]}},
        )

With the high-level Resource interface, use :class:`Vector` directly::

    import alternator
    from alternator.vector import Vector

    config = alternator.Config(seed_hosts=["192.168.1.1"], port=8000)
    resource_ctx = alternator.resource("dynamodb", cluster_config=config)

    with resource_ctx as resource:  # vector support enabled automatically
        table = resource.Table("embeddings")
        table.put_item(Item={"id": "item1", "embedding": Vector([0.1, 0.2, 0.3, 0.4])})
        response = table.query(
            VectorSearch={"QueryVector": Vector([0.1, 0.2, 0.3, 0.4])},
        )
        # response["Items"][0]["embedding"] will be a Vector instance
"""

from __future__ import annotations

from typing import Any

import boto3.dynamodb.types

# ---------------------------------------------------------------------------
# New botocore shapes that describe Alternator's vector search extensions.
# These are injected into the DynamoDB service model at runtime.
# ---------------------------------------------------------------------------

_VECTOR_SHAPES: dict[str, Any] = {
    # --- CreateTable / DescribeTable -----------------------------------------
    "VectorIndexes": {
        "type": "list",
        "member": {"shape": "VectorIndex"},
    },
    "VectorIndex": {
        "type": "structure",
        "members": {
            "IndexName": {"shape": "String"},
            "VectorAttribute": {"shape": "VectorAttribute"},
            "Projection": {"shape": "Projection"},
            "SimilarityFunction": {"shape": "String"},
            # Read-only fields returned by DescribeTable / CreateTable output
            "IndexStatus": {"shape": "String"},
            "Backfilling": {"shape": "BooleanObject"},
        },
        "required": ["IndexName", "VectorAttribute"],
    },
    "VectorAttribute": {
        "type": "structure",
        "members": {
            "AttributeName": {"shape": "String"},
            "Dimensions": {"shape": "Integer"},
        },
        "required": ["AttributeName", "Dimensions"],
    },
    # --- UpdateTable ---------------------------------------------------------
    "VectorIndexUpdates": {
        "type": "list",
        "member": {"shape": "VectorIndexUpdate"},
    },
    "VectorIndexUpdate": {
        "type": "structure",
        "members": {
            "Create": {"shape": "CreateVectorIndexAction"},
            "Delete": {"shape": "DeleteVectorIndexAction"},
        },
    },
    "CreateVectorIndexAction": {
        "type": "structure",
        "members": {
            "IndexName": {"shape": "String"},
            "VectorAttribute": {"shape": "VectorAttribute"},
            "Projection": {"shape": "Projection"},
            "SimilarityFunction": {"shape": "String"},
        },
        "required": ["IndexName", "VectorAttribute"],
    },
    "DeleteVectorIndexAction": {
        "type": "structure",
        "members": {
            "IndexName": {"shape": "String"},
        },
        "required": ["IndexName"],
    },
    # --- Query ---------------------------------------------------------------
    "VectorSearch": {
        "type": "structure",
        "members": {
            "QueryVector": {"shape": "AttributeValue"},
            "ReturnScores": {"shape": "String"},
        },
        "required": ["QueryVector"],
    },
    "Score": {"type": "double"},
    "ScoresList": {
        "type": "list",
        "member": {"shape": "Score"},
    },
    # --- FLOAT32VECTOR attribute type ----------------------------------------
    "Float32VectorElement": {"type": "double"},
    "Float32VectorAttributeValue": {
        "type": "list",
        "member": {"shape": "Float32VectorElement"},
    },
}


class Vector(list[float]):
    """An optimized vector type for ScyllaDB Alternator vector search.

    Subclasses :class:`list` so it can be used everywhere a regular Python
    list is expected.  When :func:`enable_vector_support` has been called,
    the patched :class:`~boto3.dynamodb.types.TypeSerializer` will encode a
    ``Vector`` value as ``{"FLOAT32VECTOR": [...]}`` on the wire instead of
    the standard DynamoDB list encoding ``{"L": [{"N": "..."}, ...]}``.
    Conversely, ``FLOAT32VECTOR`` values received from the server are
    deserialized back into ``Vector`` instances.

    Example::

        from alternator.vector import Vector

        embedding = Vector([0.1, 0.2, 0.3, 0.4])
        table.put_item(Item={"id": "item1", "embedding": embedding})

        response = table.get_item(Key={"id": "item1"})
        assert isinstance(response["Item"]["embedding"], Vector)
    """


class _VectorTypeSerializer(boto3.dynamodb.types.TypeSerializer):
    """TypeSerializer subclass that handles :class:`Vector` values."""

    def serialize(self, value: Any) -> dict[str, Any]:  # type: ignore[override]  # noqa: ANN401 -- boto3 TypeSerializer.serialize accepts any Python value
        if isinstance(value, Vector):
            for i, element in enumerate(value):
                if isinstance(element, bool) or not isinstance(element, (int, float)):
                    raise TypeError(
                        f"Vector element at index {i} must be int or float, "
                        f"got {type(element).__name__!r}"
                    )
            return {"FLOAT32VECTOR": list(value)}
        return super().serialize(value)  # type: ignore[return-value]


class _VectorTypeDeserializer(boto3.dynamodb.types.TypeDeserializer):
    """TypeDeserializer subclass that handles FLOAT32VECTOR values."""

    def _deserialize_float32vector(self, value: Any) -> Vector:  # noqa: ANN401 -- value is the raw JSON list from the DynamoDB wire format
        return Vector(value)


def enable_vector_support(client_or_resource: Any) -> None:  # noqa: ANN401 -- accepts either a boto3 DynamoDB client or resource instance
    """Enable ScyllaDB Alternator vector search on a boto3 DynamoDB client or resource.

    Patches botocore's in-memory DynamoDB service model to accept and return
    the new parameters introduced by Alternator's vector search feature.
    Also patches :class:`boto3.dynamodb.types.TypeSerializer` and
    :class:`boto3.dynamodb.types.TypeDeserializer` globally so that the
    high-level DynamoDB Resource interface handles :class:`Vector` values
    transparently.

    This function is **idempotent**: calling it multiple times (on the same or
    different clients) is safe and has no additional effect.

    Because botocore's underlying shape data (``_shape_map``) is shared across
    all clients created from the same boto3 session, the modifications made
    here are visible to any client that resolves the affected shapes for the
    first time after this call.  However, clients that have already cached a
    resolved shape object will retain their cached version.  In practice this
    is not a concern because this function is called on each freshly-created
    client before any requests are made.

    Args:
        client_or_resource: A boto3 DynamoDB *client* (e.g. returned by
            :func:`~alternator.client` or ``boto3.client("dynamodb")``)
            or a DynamoDB *resource* (e.g. returned by
            :func:`~alternator.resource` or
            ``boto3.resource("dynamodb")``).
    """
    # Resolve to the underlying boto3 low-level client
    if hasattr(client_or_resource, "meta") and hasattr(
        client_or_resource.meta, "client"
    ):
        # DynamoDB Resource: resource.meta.client is the low-level client
        boto_client = client_or_resource.meta.client
    else:
        boto_client = client_or_resource

    service_model = boto_client.meta.service_model
    shape_resolver = service_model._shape_resolver

    # Register new shapes (skip shapes that are already present)
    for shape_name, shape_def in _VECTOR_SHAPES.items():
        if shape_name not in shape_resolver._shape_map:
            shape_resolver._shape_map[shape_name] = shape_def
        shape_resolver._shape_cache.pop(shape_name, None)

    # Add VectorIndexes to CreateTable input
    create_table_input = service_model.operation_model("CreateTable").input_shape
    if "VectorIndexes" not in create_table_input._shape_model["members"]:
        create_table_input._shape_model["members"]["VectorIndexes"] = {
            "shape": "VectorIndexes"
        }
    create_table_input._cache.pop("members", None)

    # Add VectorIndexUpdates to UpdateTable input
    update_table_input = service_model.operation_model("UpdateTable").input_shape
    if "VectorIndexUpdates" not in update_table_input._shape_model["members"]:
        update_table_input._shape_model["members"]["VectorIndexUpdates"] = {
            "shape": "VectorIndexUpdates"
        }
    update_table_input._cache.pop("members", None)

    # Add VectorSearch to Query input and Scores to Query output
    query_op = service_model.operation_model("Query")
    query_input = query_op.input_shape
    if "VectorSearch" not in query_input._shape_model["members"]:
        query_input._shape_model["members"]["VectorSearch"] = {"shape": "VectorSearch"}
    query_input._cache.pop("members", None)

    query_output = query_op.output_shape
    if "Scores" not in query_output._shape_model["members"]:
        query_output._shape_model["members"]["Scores"] = {"shape": "ScoresList"}
    query_output._cache.pop("members", None)

    # Add VectorIndexes to TableDescription (DescribeTable / CreateTable output)
    table_desc = shape_resolver.get_shape_by_name("TableDescription")
    if "VectorIndexes" not in table_desc._shape_model["members"]:
        table_desc._shape_model["members"]["VectorIndexes"] = {"shape": "VectorIndexes"}
    table_desc._cache.pop("members", None)
    shape_resolver._shape_cache.pop("TableDescription", None)

    # Add FLOAT32VECTOR to AttributeValue
    attr_value = shape_resolver.get_shape_by_name("AttributeValue")
    if "FLOAT32VECTOR" not in attr_value._shape_model["members"]:
        attr_value._shape_model["members"]["FLOAT32VECTOR"] = {
            "shape": "Float32VectorAttributeValue"
        }
    attr_value._cache.pop("members", None)
    shape_resolver._shape_cache.pop("AttributeValue", None)

    # Patch TypeSerializer / TypeDeserializer on the resource's injector
    # (per-resource, not process-wide).  The _injector is only present on the
    # high-level DynamoDB Resource; low-level clients don't use TypeSerializer.
    if hasattr(client_or_resource, "_injector"):
        injector = client_or_resource._injector
        if not isinstance(injector._serializer, _VectorTypeSerializer):
            injector._serializer = _VectorTypeSerializer()
        if not isinstance(injector._deserializer, _VectorTypeDeserializer):
            injector._deserializer = _VectorTypeDeserializer()
