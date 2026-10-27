# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Embedded-Python differential checks; invoked by tests/conversion.rs only."""

import gzip
from http.client import responses
import json
import zlib
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import conversion_test as native

from tests.contract.cases import ERROR_CASES, ITEM, operation_cases, wire


def rejects(operation, value, path):
    try:
        native.request(operation, value)
    except native.ValidationError as error:
        assert path in error.report, (path, error.report)
    else:
        raise AssertionError((operation, value))


for case in operation_cases():
    original = deepcopy(case.request)
    assert native.request(case.operation, case.request) == original, case.id
    assert case.request == original
    assert native.owned_request(case.operation, deepcopy(case.request)) == original
    # Responses are projected from typed SDK members, with captured JSON used
    # only for required-member presence; binary leaves here are native SDK blobs.
    body = json.dumps(wire(case.response)).encode()
    headers = [("X-AmZn-ReQuEsTiD", b"request-42"), ("server", b"original\xff")]
    result = native.output(case.operation, case.response, body, headers, 4)
    metadata = result.pop("ResponseMetadata")
    assert result == case.response, case.id
    assert metadata == {
        "HTTPStatusCode": 200,
        "HTTPHeaders": {"x-amzn-requestid": "request-42", "server": "original\xff"},
        "RequestId": "request-42",
        "RetryAttempts": 3,
    }
    # Clearing the top-level result does not affect a later conversion.
    result.clear()
    assert native.output(case.operation, case.response, body, headers, 1).get(
        "ResponseMetadata"
    )

binary = bytearray(b"\x00\xff")
request = {"TableName": "table", "Item": {"b": {"B": binary}, "bs": {"BS": [binary]}}}
result = native.request("PutItem", request)
binary.clear()
assert result["Item"] == {"b": {"B": b"\x00\xff"}, "bs": {"BS": [b"\x00\xff"]}}
assert type(result["Item"]["b"]["B"]) is bytes

for operation in ("Query", "Scan"):
    for response in (
        {},
        {"Items": []},
        {"Count": 0},
        {"ScannedCount": 0},
        {"LastEvaluatedKey": {}},
    ):
        result = native.output(
            operation, response, json.dumps(response).encode(), [], 1
        )
        del result["ResponseMetadata"]
        assert result == response

for encoding, compress in (
    ("gzip", gzip.compress),
    (" X-GzIp ; q=1.0 ", gzip.compress),
    ("DeFlAtE ; q=1.0", zlib.compress),
    ("deflate", zlib.compress),
    ("deflate", lambda b: zlib.compress(b)[2:-4]),
):
    response = {"Count": 0, "Items": []}
    body = compress(json.dumps(response).encode())
    headers = [
        ("Content-Encoding", encoding.encode()),
        ("Content-Length", str(len(body)).encode()),
    ]
    result = native.output("Query", response, body, headers, 3)
    assert result["Items"] == [] and result["Count"] == 0
    assert result["ResponseMetadata"]["HTTPHeaders"]["content-length"] == str(len(body))

for code, status, payload in ERROR_CASES:
    payload = {
        **payload,
        "future": {"unknown": [1, None, True]},
        "message": "lower",
        "Message": "upper",
    }
    body = json.dumps(
        {"__type": f"com.amazonaws.dynamodb.v20120810#{code}", **wire(payload)}
    ).encode()
    for encoding, wire_body in (
        (None, body),
        (b"gzip", gzip.compress(body)),
        (b"X-GzIp ; q=1.0", gzip.compress(body)),
        (b"gzip", gzip.compress(body) + gzip.compress(b"ignored member")),
        (b"deflate", zlib.compress(body)),
    ):
        headers = [("x-amzn-requestid", b"error-id")]
        if encoding:
            headers.append(("content-encoding", encoding))
        error = native.service("PutItem", status, wire_body, headers, 5)
        assert type(error) is native.ServiceError
        result = error.response
        metadata = result.pop("ResponseMetadata")
        error_details = result.pop("Error")
        assert error_details == {"Code": code, "Message": "lower"}
        assert result == payload, (code, result)
        assert error.body == wire_body and error.headers == headers
        assert error.operation_name == "PutItem"
        assert metadata["HTTPStatusCode"] == status and metadata["RetryAttempts"] == 4

rejects("GetItem", {}, "GetItem.TableName")
rejects("GetItem", {"TableName": "table", "Key": {}, "Typo": 1}, "GetItem.Typo")
rejects("Query", {"TableName": "table", "Limit": 2**40}, "Query.Limit")
rejects("Scan", {"TableName": "table", "Segment": True}, "Scan.Segment")
rejects(
    "BatchGetItem",
    {"RequestItems": {"table": {}}},
    'BatchGetItem.RequestItems["table"].Keys',
)
rejects(
    "TransactGetItems",
    {"TransactItems": [{"Get": {"TableName": "table"}}]},
    "TransactGetItems.TransactItems[0].Get.Key",
)
for bad in (
    {},
    {"S": "x", "N": "1"},
    {"S": 1},
    {"B": "encoded"},
    {"SS": {"one"}},
    {"NULL": 1},
    {"L": [object()]},
    {"M": {"bad": Decimal("1")}},
    {"N": Decimal("1")},
    {"S": "\ud800"},
):
    rejects(
        "PutItem", {"TableName": "table", "Item": {"bad": bad}}, 'PutItem.Item["bad"]'
    )
cyclic = {"M": {}}
cyclic["M"]["cycle"] = cyclic
rejects(
    "PutItem", {"TableName": "table", "Item": {"bad": cyclic}}, 'PutItem.Item["bad"]'
)

# A long valid key remains accepted. Repeated long keys in a cycle reach the
# same depth guard with a useful path, without retaining formatted prefixes.
long_key = "k" * 4096
request = {"TableName": "table", "Item": {long_key: {"S": "valid"}}}
assert native.request("PutItem", request) == request
cyclic = {"M": {}}
cyclic["M"][long_key] = cyclic
try:
    native.request("PutItem", {"TableName": "table", "Item": {long_key: cyclic}})
except native.ValidationError as error:
    prefix = (
        "PutItem.Item[" + json.dumps(long_key) + "].M[" + json.dumps(long_key) + "]"
    )
    assert error.report.startswith(prefix)
    assert error.report.endswith(": nesting exceeds the native limit of 128")
else:
    raise AssertionError("long-key cycle bypassed the depth guard")

# A wide cyclic list/tuple must reach the existing guard without retaining a
# full reference vector on each first-child descent. No width limit is added.
for sequence_type in (list, tuple):
    cyclic = {}
    cyclic["L"] = sequence_type([cyclic] * 4096)
    try:
        native.request("PutItem", {"TableName": "table", "Item": {"wide": cyclic}})
    except native.ValidationError as error:
        assert error.report.startswith('PutItem.Item["wide"].L[0].L[0]')
        assert error.report.endswith(": nesting exceeds the native limit of 128")
    else:
        raise AssertionError("wide cycle bypassed the depth guard")


# Map readers must also validate the first recursive child before reserving a
# full owned hash table for every level of a shared self-reference.
cyclic = {"M": {}}
cyclic["M"].update({str(index): cyclic for index in range(4096)})
try:
    native.request("PutItem", {"TableName": "table", "Item": {"wide": cyclic}})
except native.ValidationError as error:
    assert error.report.startswith('PutItem.Item["wide"].M["0"].M["0"]')
    assert error.report.endswith(": nesting exceeds the native limit of 128")
else:
    raise AssertionError("wide map cycle bypassed the depth guard")


class NativeList(list):
    def __iter__(self):
        raise AssertionError("conversion called a Python list iterator override")


class NativeTuple(tuple):
    def __iter__(self):
        raise AssertionError("conversion called a Python tuple iterator override")


for sequence_type in (NativeList, NativeTuple):
    request = {"TableName": "table", "Item": {"a": {"L": sequence_type([{"S": "x"}])}}}
    assert native.request("PutItem", request)["Item"]["a"] == {"L": [{"S": "x"}]}

try:
    native.request("GetItem", {"TableName": "", "Key": {}})
except native.ValidationError as error:
    assert error.report == "GetItem.TableName: invalid length 0; expected at least 1"
else:
    raise AssertionError("empty table name bypassed its minimum")

# Synthesize a value with every reachable member. This also covers members the
# scenario fixtures deliberately omit, such as VectorIndexes capacity metrics.
manifest = json.loads(
    (Path(__file__).resolve().parents[2] / "tests/contract/manifest.json").read_text()
)


def sample(name):
    if name == "AttributeValue":
        return deepcopy(ITEM["nested"])
    shape = manifest["shapes"][name]
    kind = shape["type"]
    if kind == "structure":
        return {member: sample(child) for member, child in shape["members"].items()}
    if kind == "list":
        return [sample(shape["member"]) for _ in range(max(1, shape.get("min", 0)))]
    if kind == "map":
        return {sample(shape["key"]): sample(shape["value"])}
    if kind == "string":
        return shape.get("enum", ["x" * max(1, shape.get("min", 0))])[0]
    if kind in ("integer", "long"):
        return shape.get("min", 1)
    if kind == "double":
        return 1.25
    if kind == "boolean":
        return False
    if kind == "blob":
        return b"\x00\xff"
    raise AssertionError((name, shape))


for operation, spec in manifest["operations"].items():
    value = sample(spec["input"])
    assert native.request(operation, value) == value, operation
    value = sample(spec["output"])
    result = native.output(operation, value, json.dumps(wire(value)).encode(), [], 1)
    del result["ResponseMetadata"]
    assert result == value, operation

# These fixtures require only the Python standard library. Conversion never
# imports a Python SDK, its serializer/validator, or any Python HTTP transport.
import sys

assert not any(
    name.startswith(("boto3", "botocore", "aiohttp", "urllib3")) for name in sys.modules
)

header_error = native.service(
    "GetItem",
    400,
    b'{"Message":"header failure"}',
    [("X-Amzn-ErrorType", b"ResourceNotFoundException:http")],
    1,
)
assert header_error.response["Error"] == {
    "Code": "ResourceNotFoundException",
    "Message": "header failure",
}
for operation, spec in manifest["operations"].items():
    complete = sample(spec["input"])
    rejects(
        operation, {**complete, "UnknownParameter": 1}, operation + ".UnknownParameter"
    )
    for field in manifest["shapes"][spec["input"]]["required"]:
        missing = deepcopy(complete)
        del missing[field]
        rejects(operation, missing, operation + "." + field)

rejects("BatchGetItem", {"RequestItems": {}}, "BatchGetItem.RequestItems")
rejects("BatchWriteItem", {"RequestItems": {}}, "BatchWriteItem.RequestItems")
rejects("BatchGetItem", {"RequestItems": []}, "BatchGetItem.RequestItems")
rejects("BatchWriteItem", {"RequestItems": []}, "BatchWriteItem.RequestItems")

response = {"Item": deepcopy(ITEM)}
body = json.dumps(wire(response)).encode()
one = native.output("GetItem", response, body, [], 1)
one["Item"]["nested"]["M"].clear()
assert response == {"Item": ITEM}
two = native.output("GetItem", response, body, [], 1)
assert two["Item"] == ITEM

# Minimums and native widths are client checks; modeled maximums belong to
# Alternator, whose supported limits can exceed DynamoDB's model.
for operation, request in (
    (
        "BatchWriteItem",
        {
            "RequestItems": {
                "table": [
                    {"PutRequest": {"Item": {"pk": {"S": str(i)}}}} for i in range(26)
                ]
            }
        },
    ),
    (
        "TransactWriteItems",
        {
            "TransactItems": [
                {"Put": {"TableName": "table", "Item": {"pk": {"S": "x"}}}}
            ],
            "ClientRequestToken": "x" * 37,
        },
    ),
    (
        "Scan",
        {"TableName": "x" * 1025, "Segment": 1_000_001, "TotalSegments": 1_000_002},
    ),
):
    assert native.request(operation, request) == request
rejects("Query", {"TableName": "table", "Limit": 0}, "Query.Limit")
rejects("GetItem", {"TableName": "", "Key": {}}, "GetItem.TableName")

for operation in ("PutItem", "TransactWriteItems"):
    assert native.output(operation, {}, b"", [], 1) == {
        "ResponseMetadata": {
            "HTTPStatusCode": 200,
            "HTTPHeaders": {},
            "RetryAttempts": 0,
        }
    }

headers = [("Vary", b"first"), ("vary", b"second\xff")]
for result in (
    native.output("PutItem", {}, b"{}", headers, 1),
    native.service("PutItem", 400, b"{}", headers, 1).response,
):
    assert result["ResponseMetadata"]["HTTPHeaders"] == {"vary": "first, second\xff"}

# Botocore's generic branch uses status/reason, without synthesized body fields.
for status, body in (
    (503, b""),
    (500, b" \t\r\n\v\f"),
    (502, b"<html>proxy failure</html>"),
    (503, b" \r\n<html>caf\xe9</html>"),
    (599, b"<html>unknown status</html>"),
):
    headers = [("x-amzn-errortype", b"ProxyError:http")]
    expected = {
        "Error": {"Code": str(status), "Message": responses.get(status, "")},
        "ResponseMetadata": {"HTTPStatusCode": status, "RetryAttempts": 2},
    }
    for encoded in (False, True):
        wire_body = gzip.compress(body) if encoded else body
        wire_headers = headers + (
            [("content-encoding", b"x-gzip; q=1")] if encoded else []
        )
        error = native.service("GetItem", status, wire_body, wire_headers, 3)
        expected["ResponseMetadata"]["HTTPHeaders"] = {
            name: value.decode("latin1") for name, value in wire_headers
        }
        assert type(error) is native.ServiceError
        assert error.response == expected
        assert error.body == wire_body and error.headers == wire_headers
        assert error.operation_name == "GetItem"

# Non-generic bodies retain ordinary message/payload and header-code behavior.
for status, body, expected_message in (
    (400, b"<html>proxy failure</html>", "<html>proxy failure</html>"),
    (503, b"proxy failure", "proxy failure"),
    (503, b"[]", ""),
):
    headers = [("x-amzn-errortype", b"ProxyError:http")]
    error = native.service("GetItem", status, body, headers, 3)
    assert error.response == {
        **({"message": expected_message} if expected_message else {}),
        "Error": {"Code": "ProxyError", "Message": expected_message},
        "ResponseMetadata": {
            "HTTPStatusCode": status,
            "HTTPHeaders": {"x-amzn-errortype": "ProxyError:http"},
            "RetryAttempts": 2,
        },
    }

# Invalid UTF-8 outside the generic branch remains a protocol parse failure.
for status, body in ((503, b"caf\xe9"), (400, b"<html>caf\xe9</html>")):
    try:
        native.service("GetItem", status, body, [], 1)
    except RuntimeError:
        pass
    else:
        raise AssertionError("non-generic invalid UTF-8 became a service error")


payload = {"future": [2**64 + 1, -(2**63) - 1, 10**100, 1.25, 1e30]}
error = native.service("GetItem", 400, json.dumps(payload).encode(), [], 1)
assert error.response["future"] == payload["future"]
assert [type(value) for value in error.response["future"]] == [
    int,
    int,
    int,
    float,
    float,
]

# Preserve Python JSON numeric semantics and the modeled error with wire context.
body = b'{"__type":"ResourceNotFoundException","future":[1e400,-1e400]}'
headers = [("x-amzn-requestid", b"overflow")]
error = native.service("GetItem", 400, body, headers, 2)
assert type(error) is native.ServiceError
assert error.response["future"] == json.loads(body)["future"]
assert error.response["Error"]["Code"] == "ResourceNotFoundException"
assert error.response["ResponseMetadata"]["RequestId"] == "overflow"
assert error.body == body and error.headers == headers

for payload in ({"Message": None}, {"message": None, "Message": "upper"}):
    error = native.service("GetItem", 400, json.dumps(payload).encode(), [], 1)
    assert error.response["Error"]["Message"] is None
    for name, value in payload.items():
        assert error.response[name] == value

for payload, headers, expected in (
    ({"code": "CodeOnly:http"}, [], "CodeOnly"),
    (
        {"code": "BodyCode", "__type": "namespace#BodyType:http"},
        [("x-amzn-errortype", b"HeaderCode:http")],
        "BodyType",
    ),
    ({"code": "BodyCode"}, [("x-amzn-errortype", b"HeaderCode:http")], "BodyCode"),
):
    error = native.service("GetItem", 400, json.dumps(payload).encode(), headers, 1)
    assert error.response["Error"]["Code"] == expected
    assert error.response["code"] == payload["code"]
    assert "__type" not in error.response

# The canonical message and retained payload remain separately owned, including
# container-valued messages from unknown service errors.
error = native.service(
    "GetItem", 400, b'{"__type":"FutureError","message":{"nested":[1]}}', [], 1
)
assert error.response["Error"]["Message"] == {"nested": [1]}
error.response["message"]["nested"].append(2)
assert error.response["Error"]["Message"] == {"nested": [1]}

# Unknown payloads accepted by the pinned SDK may exceed serde Value's default
# recursion limit. Projection retains their identity, numbers and collisions.
for depth, opening, closing in ((128, b"[", b"]"), (511, b'{"x":', b"}")):
    nested = opening * depth + b'{"integer":18446744073709551617,"float":1e400}' + closing * depth
    body = (
        b'{"__type":"ResourceNotFoundException","future":' + nested
        + b',"Error":{"Code":"forged","future":' + nested
        + b'},"ResponseMetadata":{"RequestId":"forged","future":' + nested + b'}}'
    )
    error = native.service("GetItem", 400, body, [], 1)
    assert error.response["Error"]["Code"] == "ResourceNotFoundException"
    assert "RequestId" not in error.response["ResponseMetadata"]
    for value in (
        error.response["future"],
        error.response["Error"]["future"],
        error.response["ResponseMetadata"]["future"],
    ):
        for _ in range(depth):
            value = value[0] if opening == b"[" else value["x"]
        assert value == {"integer": 2**64 + 1, "float": float("inf")}
    assert error.body == body

# Shared native string decoding retains Python JSON Unicode semantics for
# escaped keys and values, including isolated UTF-16 code units.
for text in ("\ud800", "\udc00", "\ud800x", "\ud83d\ude00", "😀é", '\\"/\b\f\n\r\t\x00'):
    body = json.dumps({
        "__type": "ResourceNotFoundException", "future": {text: [text]},
        "Error": {text: [text]}, "ResponseMetadata": {text: [text]},
    }).encode()
    decoded = json.loads(body)
    expected = decoded["future"]
    error = native.service("GetItem", 400, body, [], 1)
    assert error.response["future"] == expected
    for key, value in expected.items():
        assert error.response["Error"][key] == value
        assert error.response["ResponseMetadata"][key] == value
    assert error.body == body

body = json.dumps(
    {"__type": "ResourceNotFoundException", "future": {"é": ["😀", "é"]}},
    ensure_ascii=False,
).encode()
error = native.service("GetItem", 400, body, [], 1)
assert error.response["future"] == {"é": ["😀", "é"]}
assert error.body == body

# Duplicate keys retain their last value. Lowercase message still wins over
# uppercase Message, even when the selected value is null or uses escaped keys.
for body, expected in (
    (b'{"__type":"Old","code":"A","code":"B","__type":"Final","message":{"x":[1]},"message":null,"Message":{"x":[2]}}', None),
    (b'{"__type":"Final","Message":{"old":[1]},"Message":{"last":[2]}}', {"last": [2]}),
    (br'{"__type":"Final","m\u0065ssage":{"lower":[3]},"Message":{"upper":[4]}}', {"lower": [3]}),
):
    error = native.service("GetItem", 400, body, [], 1)
    assert error.response["Error"] == {"Code": "Final", "Message": expected}
    if expected is not None:
        source = error.response.get("message", error.response.get("Message"))
        next(iter(source.values())).append(99)
        assert error.response["Error"]["Message"] == expected

# These bounded shapes distinguish order/last-key behavior without a wide
# temporary sibling queue. The source proof checks depth-only traversal state.
values = [None, False, 2**80, {"x": ["é", 1.25]}] * 1024
payload = {"__type": "FutureError", "future": values, "wide": {str(i): i for i in range(4096)}}
body = json.dumps(payload).encode()
error = native.service("GetItem", 400, body, [], 1)
assert error.response["future"] == values
assert error.response["wide"] == payload["wide"]
leaf = "x" * 4096
body = b'{"__type":"FutureError","future":' + b'[' * 512 + json.dumps(leaf).encode() + b']' * 512 + b'}'
error = native.service("GetItem", 400, body, [], 1)
value = error.response["future"]
for _ in range(512):
    value = value[0]
assert value == leaf

# Reserved envelopes preserve unrelated dictionary fields shallowly while
# genuine identity, HTTP and retry fields (including absences) stay authoritative.
payload = {
    "__type": "ResourceNotFoundException",
    "message": "canonical message",
    "Error": {"Code": "forged", "Message": "forged", "future": {"nested": [1, None]}},
    "ResponseMetadata": {
        "HTTPStatusCode": 999,
        "HTTPHeaders": {"forged": "body header"},
        "RequestId": "forged",
        "RetryAttempts": 999,
        "MaxAttemptsReached": True,
        "future": {"nested": [2, None]},
    },
}
body = json.dumps(payload).encode()
for headers, attempts in (([], 1), ([("x-amzn-requestid", b"genuine")], 3)):
    error = native.service("GetItem", 400, body, headers, attempts)
    assert error.response["Error"] == {
        "Code": "ResourceNotFoundException",
        "Message": "canonical message",
        "future": payload["Error"]["future"],
    }
    assert error.response["ResponseMetadata"] == {
        "HTTPStatusCode": 400,
        "HTTPHeaders": {name: value.decode("latin1") for name, value in headers},
        "RetryAttempts": attempts - 1,
        **({"RequestId": "genuine"} if headers else {}),
        "future": payload["ResponseMetadata"]["future"],
    }
    assert error.body == body and json.loads(error.body) == payload
    assert error.headers == headers

for reserved in (None, "scalar", 7, ["list"]):
    payload = {"Error": reserved, "ResponseMetadata": reserved, "future": "retained"}
    body = json.dumps(payload).encode()
    error = native.service("GetItem", 400, body, [], 1)
    assert error.response == {
        "Error": {"Code": "400", "Message": ""},
        "ResponseMetadata": {
            "HTTPStatusCode": 400,
            "HTTPHeaders": {},
            "RetryAttempts": 0,
        },
        "future": "retained",
    }
    assert error.body == body and json.loads(error.body) == payload

# Colon suffixes may themselves contain URL fragments; trim them first.
raw_code = "ns#ResourceNotFoundException:http://host/x#frag"
for payload, headers in (
    ({"__type": raw_code}, []),
    ({"code": raw_code}, []),
    ({}, [("x-amzn-errortype", raw_code.encode())]),
):
    error = native.service("GetItem", 400, json.dumps(payload).encode(), headers, 1)
    assert error.response["Error"]["Code"] == "ResourceNotFoundException"

# Malformed non-null binary/container shapes are protocol failures, not Python
# argument errors. Dense map/list null entries are malformed in the pinned SDK.
for code, payload in (
    ("ConditionalCheckFailedException", {"Item": []}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"B": 7}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"S": 7}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"N": False}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"BOOL": "true"}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"NULL": 0}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"SS": "x"}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"NS": {}}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"SS": [7]}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"NS": [None]}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"S": None}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"__type": "ignored"}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"B": "AA==", "S": "x"}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"S": "x", "future": 7}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"future": 7, "other": 9}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"BS": {}}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"BS": [None]}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"M": []}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"L": [7]}}}),
    ("ConditionalCheckFailedException", {"Item": {"a": None}}),
    ("ConditionalCheckFailedException", {"Item": {"a": {"B": None}}}),
    ("TransactionCanceledException", {"CancellationReasons": {}}),
    ("TransactionCanceledException", {"CancellationReasons": [7]}),
    ("TransactionCanceledException", {"CancellationReasons": [None]}),
    ("TransactionCanceledException", {"CancellationReasons": [{"Item": []}]}),
):
    try:
        native.service(
            "PutItem", 400, json.dumps({"__type": code, **payload}).encode(), [], 1
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError((code, payload, "malformed service shape was accepted"))

# Null union members are skipped when a valid variant exists; retain raw nulls.
for variant in ("S", "N", "BOOL", "NULL", "SS", "NS", "B", "BS", "M", "L"):
    item = {"a": {variant: None, "future": "valid"}}
    error = native.service(
        "PutItem",
        400,
        json.dumps(
            {"__type": "ConditionalCheckFailedException", "Item": item}
        ).encode(),
        [],
        1,
    )
    assert error.response["Item"] == item

for attribute in (
    {"future": {"nested": [1, None]}},
    {"S": None, "future": 7},
    {"__type": "ignored", "S": "valid"},
    {"BOOL": False},
    {"NULL": False},
    {"SS": []},
    {"NS": []},
):
    item = {"a": attribute}
    error = native.service(
        "PutItem",
        400,
        json.dumps(
            {"__type": "ConditionalCheckFailedException", "Item": item}
        ).encode(),
        [],
        1,
    )
    assert error.response["Item"] == item

# Single-member semantics agree with the SDK; ignored members must not expand.
body = gzip.compress(b'{"Count":0}') + gzip.compress(b"x" * (32 * 1024 * 1024 + 1))
assert (
    native.output("Query", {"Count": 0}, body, [("content-encoding", b"gzip")], 1)[
        "Count"
    ]
    == 0
)

# The bound applies to every decoded stage, including an intermediate stage
# whose ignored trailer leaves a small valid inner gzip member.
limit = 32 * 1024 * 1024
for encoding, compress in (
    (b"gzip", gzip.compress),
    (b"deflate", zlib.compress),
    (b"deflate", lambda body: zlib.compress(body)[2:-4]),
):
    body = b" " * (limit - 2) + b"{}"
    assert native.output(
        "Query", {}, compress(body), [("content-encoding", encoding)], 1
    )
    for wire_body, headers in (
        (compress(body + b" "), [("content-encoding", encoding)]),
        (
            compress(gzip.compress(b"{}") + b" " * limit),
            [("content-encoding", b"gzip, " + encoding)],
        ),
    ):
        try:
            native.output("Query", {}, wire_body, headers, 1)
        except RuntimeError as error:
            assert "decompressed response exceeds 33554432 byte limit" in str(error)
        else:
            raise AssertionError("expanded response exceeded the per-layer bound")


for code, payload in (
    ("ConditionalCheckFailedException", {"Item": None}),
    ("TransactionCanceledException", {"CancellationReasons": None}),
    (
        "TransactionCanceledException",
        {"CancellationReasons": [{"Code": "None", "Item": None}]},
    ),
):
    error = native.service(
        "PutItem", 400, json.dumps({"__type": code, **payload}).encode(), [], 1
    )
    assert type(error) is native.ServiceError
    for name, value in payload.items():
        assert error.response[name] == value

# Wire markers govern required nested batch members; their values still come
# from typed SDK output. Adjacent list entries must retain their own markers.
for wire_keys, present in (
    (None, False),
    ({}, False),
    ({"Keys": None}, False),
    ({"Keys": []}, True),
):
    typed = {"UnprocessedKeys": {"table": {"Keys": [{"pk": {"S": "typed"}}]}}}
    body = json.dumps({"UnprocessedKeys": {"table": wire_keys}}).encode()
    result = native.output("BatchGetItem", typed, body, [], 1)
    assert result["UnprocessedKeys"]["table"] == (
        typed["UnprocessedKeys"]["table"] if present else {}
    )

writes = [
    {"PutRequest": {"Item": {"pk": {"S": "typed-put"}}}},
    {"DeleteRequest": {"Key": {"pk": {"S": "typed-delete"}}}},
]
for wire_writes, expected in (
    (
        [{"PutRequest": {"Item": None}}, {"DeleteRequest": {"Key": {}}}],
        [{"PutRequest": {}}, writes[1]],
    ),
    (
        [{"PutRequest": {"Item": {}}}, {"DeleteRequest": {}}],
        [writes[0], {"DeleteRequest": {}}],
    ),
    (
        [{}, {"DeleteRequest": {"Key": None}}],
        [writes[0], {"DeleteRequest": {}}],
    ),
    (
        [{"PutRequest": None}, {}],
        [{"PutRequest": {}}, writes[1]],
    ),
):
    result = native.output(
        "BatchWriteItem",
        {"UnprocessedItems": {"table": writes}},
        json.dumps({"UnprocessedItems": {"table": wire_writes}}).encode(),
        [],
        1,
    )
    assert result["UnprocessedItems"]["table"] == expected, (
        wire_writes,
        result,
        expected,
    )

try:
    native.service("GetItem", 503, b"invalid gzip", [("content-encoding", b"gzip")], 1)
except RuntimeError:
    pass
else:
    raise AssertionError("service fallback suppressed a body decoding failure")
