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

"""Fresh boto-style request/response pairs; no native dependency is imported."""

from __future__ import annotations

import base64
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import Any

TABLE = "contract"
KEY = {"pk": {"S": "key"}}
ITEM = {
    **KEY,
    "nested": {
        "M": {
            "list": {
                "L": [
                    {"NULL": True},
                    {"BOOL": False},
                    {"M": {}},
                    {"L": []},
                    {"S": ""},
                    {"B": b""},
                ]
            }
        }
    },
    "blob": {"B": b"\x00\xffbinary"},
    "number": {"N": "12345678901234567890123456789012345678"},
    "exponent": {"N": "-1.234500E-100"},
    "strings": {"SS": ["second", "first"]},
    "numbers": {"NS": ["1.00", "2E+3"]},
    "blobs": {"BS": [b"a", b"\x00\xff"]},
}
CAPACITY = {
    "TableName": TABLE,
    "CapacityUnits": 1.5,
    "ReadCapacityUnits": 0.5,
    "WriteCapacityUnits": 1.0,
    "Table": {"CapacityUnits": 1.0},
    "LocalSecondaryIndexes": {"lsi": {"CapacityUnits": 0.25}},
    "GlobalSecondaryIndexes": {"gsi": {"CapacityUnits": 0.25}},
}
METRICS = {"ItemCollectionKey": KEY, "SizeEstimateRangeGB": [0.0, 1.25]}


def wire(value: Any) -> Any:  # noqa: ANN401 -- recursive low-level DynamoDB values
    """Encode binary leaves exactly once for fake HTTP JSON fixtures."""
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, dict):
        return {k: wire(v) for k, v in value.items()}
    if isinstance(value, list):
        return [wire(v) for v in value]
    return value


@dataclass(frozen=True)
class OperationCase:
    """A single independent request with expected parsed response members."""

    method: str
    operation: str
    request: dict[str, Any]
    response: dict[str, Any]
    name: str = "modern"

    @property
    def id(self) -> str:
        return f"{self.method}-{self.name}"


def operation_cases() -> list[OperationCase]:
    """Exercise every data operation, expressions, batch shapes and all tx verbs."""
    expressions = {
        "ExpressionAttributeNames": {"#v": "value"},
        "ExpressionAttributeValues": {":v": {"S": "value"}},
    }
    condition = {
        "ConditionExpression": "#v = :v",
        **expressions,
        "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
    }
    write = {"ReturnConsumedCapacity": "INDEXES", "ReturnItemCollectionMetrics": "SIZE"}
    key = {"TableName": TABLE, "Key": KEY}
    items = {
        TABLE: {
            "Keys": [KEY],
            "ConsistentRead": True,
            "ProjectionExpression": "#p",
            "ExpressionAttributeNames": {"#p": "pk"},
        }
    }
    writes = {TABLE: [{"PutRequest": {"Item": ITEM}}, {"DeleteRequest": {"Key": KEY}}]}
    data = [
        OperationCase(
            "get_item",
            "GetItem",
            {
                **key,
                "ConsistentRead": False,
                "ProjectionExpression": "#p",
                "ExpressionAttributeNames": {"#p": "pk"},
                "ReturnConsumedCapacity": "TOTAL",
            },
            {"Item": ITEM, "ConsumedCapacity": CAPACITY},
        ),
        OperationCase(
            "put_item",
            "PutItem",
            {
                "TableName": TABLE,
                "Item": ITEM,
                "ReturnValues": "ALL_OLD",
                **condition,
                **write,
            },
            {
                "Attributes": ITEM,
                "ConsumedCapacity": CAPACITY,
                "ItemCollectionMetrics": METRICS,
            },
        ),
        OperationCase(
            "update_item",
            "UpdateItem",
            {
                **key,
                **condition,
                **write,
                "UpdateExpression": "SET #v = :v",
                "ReturnValues": "ALL_NEW",
            },
            {
                "Attributes": ITEM,
                "ConsumedCapacity": CAPACITY,
                "ItemCollectionMetrics": METRICS,
            },
        ),
        OperationCase(
            "delete_item",
            "DeleteItem",
            {**key, **condition, **write, "ReturnValues": "ALL_OLD"},
            {
                "Attributes": ITEM,
                "ConsumedCapacity": CAPACITY,
                "ItemCollectionMetrics": METRICS,
            },
        ),
        OperationCase(
            "query",
            "Query",
            {
                "TableName": TABLE,
                "IndexName": "gsi",
                "KeyConditionExpression": "#v = :v",
                "FilterExpression": "#v = :v",
                **expressions,
                "Select": "ALL_PROJECTED_ATTRIBUTES",
                "Limit": 2,
                "ConsistentRead": False,
                "ScanIndexForward": False,
                "ExclusiveStartKey": KEY,
                "ReturnConsumedCapacity": "INDEXES",
            },
            {
                "Items": [ITEM],
                "Count": 1,
                "ScannedCount": 2,
                "LastEvaluatedKey": KEY,
                "ConsumedCapacity": CAPACITY,
            },
        ),
        OperationCase(
            "scan",
            "Scan",
            {
                "TableName": TABLE,
                "IndexName": "gsi",
                "Select": "ALL_ATTRIBUTES",
                "FilterExpression": "#v = :v",
                **expressions,
                "Limit": 2,
                "ExclusiveStartKey": KEY,
                "ConsistentRead": True,
                "Segment": 0,
                "TotalSegments": 2,
                "ReturnConsumedCapacity": "TOTAL",
            },
            {
                "Items": [ITEM],
                "Count": 1,
                "ScannedCount": 2,
                "LastEvaluatedKey": KEY,
                "ConsumedCapacity": CAPACITY,
            },
        ),
        OperationCase(
            "batch_get_item",
            "BatchGetItem",
            {"RequestItems": items, "ReturnConsumedCapacity": "TOTAL"},
            {
                "Responses": {TABLE: [ITEM]},
                "UnprocessedKeys": items,
                "ConsumedCapacity": [CAPACITY],
            },
        ),
        OperationCase(
            "batch_write_item",
            "BatchWriteItem",
            {"RequestItems": writes, **write},
            {
                "UnprocessedItems": writes,
                "ConsumedCapacity": [CAPACITY],
                "ItemCollectionMetrics": {TABLE: [METRICS]},
            },
        ),
        OperationCase(
            "transact_get_items",
            "TransactGetItems",
            {
                "TransactItems": [
                    {
                        "Get": {
                            **key,
                            "ProjectionExpression": "#p",
                            "ExpressionAttributeNames": {"#p": "pk"},
                        }
                    },
                    {"Get": key},
                ],
                "ReturnConsumedCapacity": "TOTAL",
            },
            {"Responses": [{"Item": ITEM}, {}], "ConsumedCapacity": [CAPACITY]},
        ),
        OperationCase(
            "transact_write_items",
            "TransactWriteItems",
            {
                "TransactItems": [
                    {"ConditionCheck": {**key, **condition}},
                    {"Put": {"TableName": TABLE, "Item": ITEM, **condition}},
                    {"Delete": {**key, **condition}},
                    {"Update": {**key, **condition, "UpdateExpression": "SET #v = :v"}},
                ],
                "ClientRequestToken": "contract-token-0001",
                **write,
            },
            {
                "ConsumedCapacity": [CAPACITY],
                "ItemCollectionMetrics": {TABLE: [METRICS]},
            },
        ),
    ]
    # Legacy fields remain part of the public low-level SDK operation contract.
    expected = {
        "Expected": {
            "pk": {
                "Value": {"S": "key"},
                "Exists": True,
                "ComparisonOperator": "EQ",
                "AttributeValueList": [{"S": "key"}],
            }
        },
        "ConditionalOperator": "AND",
    }
    for method, operation in (
        ("put_item", "PutItem"),
        ("update_item", "UpdateItem"),
        ("delete_item", "DeleteItem"),
    ):
        request = {"TableName": TABLE, **expected}
        request.update({"Item": ITEM} if method == "put_item" else {"Key": KEY})
        if method == "update_item":
            request["AttributeUpdates"] = {
                "value": {"Action": "PUT", "Value": {"S": "v"}}
            }
        data.append(OperationCase(method, operation, request, {}, "legacy"))
    for method, operation, field in (
        ("query", "Query", "QueryFilter"),
        ("scan", "Scan", "ScanFilter"),
    ):
        request = {
            "TableName": TABLE,
            "AttributesToGet": ["pk"],
            field: {
                "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "key"}]}
            },
            "ConditionalOperator": "OR",
        }
        if method == "query":
            request["KeyConditions"] = {
                "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "key"}]}
            }
        data.append(
            OperationCase(
                method,
                operation,
                request,
                {"Items": [], "Count": 0, "ScannedCount": 0, "LastEvaluatedKey": {}},
                "legacy",
            )
        )
    data.append(
        OperationCase(
            "get_item", "GetItem", {**key, "AttributesToGet": ["pk"]}, {}, "absent"
        )
    )
    data.append(OperationCase("get_item", "GetItem", key, {"Item": {}}, "empty"))
    data.append(
        OperationCase(
            "batch_get_item",
            "BatchGetItem",
            {"RequestItems": {TABLE: {"Keys": [KEY], "AttributesToGet": ["pk"]}}},
            {"Responses": {}, "UnprocessedKeys": {}},
            "legacy",
        )
    )
    for method in ("query", "scan"):
        modern = next(case for case in data if case.method == method)
        data.append(
            OperationCase(
                method,
                modern.operation,
                {
                    **modern.request,
                    "Select": "SPECIFIC_ATTRIBUTES",
                    "ProjectionExpression": "pk",
                },
                {"Items": [], "Count": 0, "ScannedCount": 0},
                "projection",
            )
        )
    return [
        replace(case, request=deepcopy(case.request), response=deepcopy(case.response))
        for case in data
    ]


ERROR_CASES: list[tuple[str, int, dict[str, Any]]] = [
    (
        "ConditionalCheckFailedException",
        400,
        {"message": "scripted failure", "Item": ITEM},
    ),
    (
        "ProvisionedThroughputExceededException",
        400,
        {
            "message": "scripted failure",
            "ThrottlingReasons": [
                {
                    "reason": "TableWriteProvisionedThroughputExceeded",
                    "resource": "arn:aws:dynamodb:us-east-1:123456789012:table/contract",
                }
            ],
        },
    ),
    ("ResourceNotFoundException", 400, {"message": "scripted failure"}),
    ("ValidationException", 400, {}),
    ("InternalServerError", 500, {"message": "scripted failure"}),
    ("UnknownFutureException", 400, {"future": "member"}),
    (
        "TransactionCanceledException",
        400,
        {
            "Message": "scripted failure",
            "CancellationReasons": [
                {
                    "Code": "ConditionalCheckFailed",
                    "Message": "condition failed",
                    "Item": ITEM,
                },
                {"Code": "None"},
            ],
        },
    ),
]
