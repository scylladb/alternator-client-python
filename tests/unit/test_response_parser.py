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

"""Differential contracts for compiled DynamoDB success responses."""

from __future__ import annotations

import asyncio
import gzip
import inspect
import json
import subprocess
import sys
import zlib
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from decimal import Decimal
from functools import wraps
from threading import Event
from types import MappingProxyType, SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, Mock

import boto3
import botocore.session
import pytest
from aiobotocore.parsers import AioResponseParserFactory
from boto3.dynamodb.types import Binary
from botocore.model import Shape, ShapeResolver
from botocore.parsers import JSONParser, ResponseParserFactory

from alternator import Auth, Config, RetryConfig, close_client, close_resource
from alternator.async_client import close_async_client, create_async_client
from alternator.client import create_client, create_resource
from alternator.config import ResponseCompression
from alternator.core.response_parser import (
    _async_parser_class,
    _CompiledJSONParser,
    _Compiler,
    _ParserFactory,
    install_response_parser,
)
from tests.fake_server import FakeAlternatorResponse, FakeAlternatorServer

_MODEL = botocore.session.get_session().get_service_model("dynamodb")
_OPERATIONS = _MODEL.operation_names
_ITEM = {
    "binary": {"B": "AP8="},
    "set": {"BS": ["YQ==", "Yg=="]},
    "numbers": {"NS": ["1.2", "0"]},
    "strings": {"SS": ["a", "b"]},
    "nested": {"M": {"list": {"L": [{"N": "1.25"}, {"NULL": True}, {"BOOL": False}]}}},
}


def _sample(shape: Any, path: tuple[str, ...] = ()) -> Any:  # noqa: ANN401 -- SDK model/JSON
    if shape.name in path:
        return {"S": "recursive leaf"} if shape.name == "AttributeValue" else {}
    path = (*path, shape.name)
    if shape.type_name == "structure":
        return {
            s.serialization.get("name", n): _sample(s, path)
            for n, s in reversed(list(shape.members.items()))
        } | {"unknown": "discard"}
    if shape.type_name == "map":
        return {"z": _sample(shape.value, path), "a": _sample(shape.value, path)}
    if shape.type_name == "list":
        return [_sample(shape.member, path), None]
    if shape.type_name == "blob":
        return "AP8="
    if shape.type_name == "timestamp":
        return 1700000000.25
    if shape.type_name == "boolean":
        return False
    if shape.type_name in {"integer", "long", "float", "double"}:
        return 2
    return "123.450"


def _response(body: Any, status: int = 200) -> dict[str, Any]:  # noqa: ANN401 -- SDK JSON
    return {
        "body": json.dumps(body).encode(),
        "headers": {"x-amzn-requestid": "request", "X-Other": "value"},
        "status_code": status,
        "context": {"checksum": {"response_algorithm": "crc32"}},
    }


def _ordered_equal(actual: Any, expected: Any) -> None:  # noqa: ANN401 -- recursive JSON
    assert actual == expected
    if isinstance(expected, dict):
        assert list(actual) == list(expected)
        for key in expected:
            _ordered_equal(actual[key], expected[key])
    elif isinstance(expected, list):
        for a, b in zip(actual, expected, strict=True):
            _ordered_equal(a, b)


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def factories(request: pytest.FixtureRequest) -> tuple[Any, _ParserFactory]:
    original = AioResponseParserFactory() if request.param else ResponseParserFactory()
    cls = _async_parser_class() if request.param else _CompiledJSONParser
    return original, _ParserFactory(original, _MODEL, cls)


async def _parse(parser: Any, response: dict[str, Any], shape: Any) -> Any:  # noqa: ANN401 -- SDK parser
    result = parser.parse(response, shape)
    return await result if inspect.isawaitable(result) else result


@pytest.mark.parametrize("operation", _OPERATIONS)
@pytest.mark.parametrize("scenario", ["populated", "empty", "nulls"])
async def test_all_output_shapes(
    factories: tuple[Any, _ParserFactory], operation: str, scenario: str
) -> None:
    original, compiled = factories
    shape = _MODEL.operation_model(operation).output_shape
    body = _sample(shape) if shape else {}
    if scenario == "empty":
        body = {}
    elif scenario == "nulls":
        body = dict.fromkeys(body)
    response = _response(body)
    _ordered_equal(
        await _parse(compiled.create_parser("json"), response, shape),
        await _parse(original.create_parser("json"), response, shape),
    )
    assert shape is None or shape in compiled._plans


@pytest.mark.parametrize("operation", _OPERATIONS)
async def test_all_error_shapes_remain_stock(
    factories: tuple[Any, _ParserFactory], operation: str
) -> None:
    original, compiled = factories
    model = _MODEL.operation_model(operation)
    for shape in model.error_shapes:
        assert shape not in compiled._plans
        body = _sample(shape) | {
            "__type": shape.name,
            "message": "failure",
            "Item": _ITEM,
        }
        response = _response(body, 400)
        for parse_shape in (model.output_shape, shape):
            _ordered_equal(
                await _parse(compiled.create_parser("json"), response, parse_shape),
                await _parse(original.create_parser("json"), response, parse_shape),
            )


@pytest.mark.parametrize(
    "body,status",
    [
        ({"__type": "unknown#Error:detail", "Message": "failure"}, 400),
        ({}, 500),
        ("invalid", 200),
    ],
)
async def test_unknown_errors_and_non_json(
    factories: tuple[Any, _ParserFactory], body: object, status: int
) -> None:
    original, compiled = factories
    response = _response(body, status)
    if body == "invalid":
        response["body"] = b"invalid JSON"
    shape = _MODEL.operation_model("GetItem").output_shape
    _ordered_equal(
        await _parse(compiled.create_parser("json"), response, shape),
        await _parse(original.create_parser("json"), response, shape),
    )


@pytest.mark.parametrize(
    "name,value",
    [
        (
            "AttributeValue",
            {"M": {"z": {"L": [None, {"B": "AP8="}, {"M": {}}]}, "a": None}},
        ),
        ("AttributeValue", {"NS": [], "SS": [], "BS": [], "M": {}, "L": []}),
        ("AttributeValue", {"NULL": False, "BOOL": False, "S": "", "N": "1.2300"}),
        ("AttributeValue", None),
        ("AttributeValue", []),
        ("AttributeValue", "malformed"),
        ("AttributeMap", []),
        ("AttributeMap", None),
        ("AttributeValueList", {"S": "malformed"}),
        ("AttributeValueList", None),
        ("AttributeMap", {"x": {"BS": [None, "AP8="]}}),
    ],
)
def test_recursive_and_malformed_values(name: str, value: object) -> None:
    # Stock handling may accept surprising container types or raise. Preserve
    # either result, including the exception type/message; never retry a failure.
    shape = _MODEL.shape_for(name)
    parser: Any = JSONParser()
    compiled = _Compiler(parser).compile(shape)
    try:
        expected = parser._parse_shape(shape, value)
    except Exception as error:
        with pytest.raises(type(error)) as caught:
            compiled(value)
        assert str(caught.value) == str(error)
    else:
        _ordered_equal(compiled(value), expected)


@pytest.mark.parametrize("kind", ["M", "L"])
async def test_deep_recursive_values(
    factories: tuple[Any, _ParserFactory], kind: str
) -> None:
    # At the default recursion limit, stock parsing accepts this depth even on
    # Python 3.10, where comprehensions consume an additional Python frame.
    depth = 210
    value: Any = {"S": "leaf"}
    for _ in range(depth):
        value = {kind: {"child": value} if kind == "M" else [value]}
    response = _response({"Item": {"nested": value}})
    shape = _MODEL.operation_model("GetItem").output_shape
    original, compiled = factories
    expected = await _parse(original.create_parser("json"), response, shape)
    actual = await _parse(compiled.create_parser("json"), response, shape)
    # Walk iteratively so comparison does not impose its own recursion limit.
    actual_value = actual.pop("Item")["nested"]
    expected_value = expected.pop("Item")["nested"]
    _ordered_equal(actual, expected)
    for _ in range(depth):
        assert list(actual_value) == list(expected_value) == [kind]
        actual_container = actual_value[kind]
        expected_container = expected_value[kind]
        if kind == "M":
            assert list(actual_container) == list(expected_container) == ["child"]
            actual_value = actual_container["child"]
            expected_value = expected_container["child"]
        else:
            assert len(actual_container) == len(expected_container) == 1
            actual_value = actual_container[0]
            expected_value = expected_container[0]
    _ordered_equal(actual_value, expected_value)


def test_reference_traits_aliases_documents_unions_and_unknown_types() -> None:
    resolver: Any = cast(Any, ShapeResolver)(
        {
            "Root": {
                "type": "structure",
                "members": {
                    "normal": {"shape": "Value"},
                    "document": {"shape": "Value", "document": True},
                    "union": {"shape": "Value", "union": True},
                    "renamed": {"shape": "Text", "locationName": "wire"},
                },
            },
            "Value": {"type": "structure", "members": {"x": {"shape": "Text"}}},
            "Text": {"type": "string"},
        }
    )
    shape = resolver.get_shape_by_name("Root")
    value = {
        "wire": "alias",
        "union": {"future": "tag"},
        "document": {"unknown": []},
        "normal": {"x": "yes", "unknown": 0},
    }
    parser: Any = JSONParser()
    _ordered_equal(
        _Compiler(parser).compile(shape)(value), parser._parse_shape(shape, value)
    )
    # Two structurally different references bearing the same name also differ.
    compiler = _Compiler(parser)
    first = resolver.get_shape_by_name("Value")
    second = resolver.get_shape_by_name("Value", {"members": {"y": {"shape": "Text"}}})
    assert compiler.compile(first)({"x": "x", "y": "y"}) == {"x": "x"}
    assert compiler.compile(second)({"x": "x", "y": "y"}) == {"y": "y"}
    assert compiler.compile(Shape("future", {"type": "future"}))([1]) == [1]
    duplicate = resolver.get_shape_by_name(
        "Root",
        {
            "members": {
                "a": {"shape": "Text", "locationName": "same"},
                "b": {"shape": "Text", "locationName": "same"},
            }
        },
    )
    assert compiler.compile(duplicate)({"same": "v"}) == {"a": "v", "b": "v"}


def test_cache_preserves_reference_member_order_and_effective_traits() -> None:
    resolver: Any = cast(Any, ShapeResolver)(
        {
            "Root": {
                "type": "structure",
                "members": {"a": {"shape": "Text"}, "b": {"shape": "Text"}},
            },
            "Text": {"type": "string"},
        }
    )
    compiler = _Compiler(JSONParser())
    first = resolver.get_shape_by_name("Root")
    reordered = resolver.get_shape_by_name(
        "Root", {"members": {"b": {"shape": "Text"}, "a": {"shape": "Text"}}}
    )
    assert list(compiler.compile(first)({"b": "b", "a": "a"})) == ["a", "b"]
    assert list(compiler.compile(reordered)({"b": "b", "a": "a"})) == ["b", "a"]
    document = resolver.get_shape_by_name("Root")
    document.metadata["document"] = True
    assert compiler.compile(document)({"future": 1}) == {"future": 1}


async def test_custom_defaults_and_fresh_parsers(
    factories: tuple[Any, _ParserFactory],
) -> None:
    original, old_compiled = factories
    blob = MagicMock(side_effect=lambda value: ("blob", value))
    timestamp = MagicMock(side_effect=lambda value: ("time", value))
    for converter in (blob, timestamp):
        cast(MagicMock, converter.__eq__).side_effect = AssertionError(
            "compare defaults by identity"
        )
    original.set_parser_defaults(blob_parser=blob, timestamp_parser=timestamp)
    compiled = _ParserFactory(original, _MODEL, old_compiled._parser_class)
    for operation, body in [
        ("GetItem", {"Item": _ITEM}),
        ("DescribeTable", {"Table": {"CreationDateTime": 1.25}}),
    ]:
        shape = _MODEL.operation_model(operation).output_shape
        _ordered_equal(
            await _parse(compiled.create_parser("json"), _response(body), shape),
            await _parse(original.create_parser("json"), _response(body), shape),
        )
    a, b = compiled.create_parser("json"), compiled.create_parser("json")
    assert a is not b
    assert a._event_stream_parser is not b._event_stream_parser
    assert a._plans is b._plans
    with pytest.raises(TypeError):
        a._plans[shape] = lambda x: x
    original.create_parser = Mock(return_value="delegate")
    assert compiled.create_parser("rest-json") == "delegate"
    original.create_parser.assert_called_once_with("rest-json")


@pytest.mark.parametrize("default", ["absent", "none", "explicit"])
async def test_global_timestamp_default_replacement_and_restoration(
    factories: tuple[Any, _ParserFactory],
    monkeypatch: pytest.MonkeyPatch,
    default: str,
) -> None:
    original, compiled = factories
    explicit = MagicMock(side_effect=lambda value: ("explicit", value))
    replacement = MagicMock(side_effect=lambda value: ("replacement", value))
    for converter in (explicit, replacement):
        cast(MagicMock, converter.__eq__).side_effect = AssertionError(
            "compare converters by identity"
        )
        cast(MagicMock, converter.__bool__).side_effect = AssertionError(
            "do not test converter truthiness"
        )
    if default != "absent":
        original.set_parser_defaults(
            timestamp_parser=explicit if default == "explicit" else None
        )
        compiled = _ParserFactory(original, _MODEL, compiled._parser_class)
    plans = compiled._plans
    shape = _MODEL.operation_model("DescribeTable").output_shape
    response = _response({"Table": {"CreationDateTime": 1.25}})
    initial = await _parse(original.create_parser("json"), response, shape)
    assert compiled.create_parser("json")._plans is plans
    with monkeypatch.context() as patch:
        patch.setattr(botocore.parsers, "DEFAULT_TIMESTAMP_PARSER", replacement)
        parser = compiled.create_parser("json")
        stock = original.create_parser("json")
        replacement.assert_not_called()
        _ordered_equal(
            await _parse(parser, response, shape),
            await _parse(stock, response, shape),
        )
        if default == "explicit":
            assert parser._plans is plans
        else:
            assert type(parser) is type(stock)
    restored = compiled.create_parser("json")
    assert restored._plans is plans
    _ordered_equal(await _parse(restored, response, shape), initial)


@pytest.mark.parametrize("setter", ["original", "installed"])
@pytest.mark.parametrize("customized", [False, True], ids=["added", "replaced"])
async def test_updated_defaults_remain_stock(
    factories: tuple[Any, _ParserFactory], setter: str, customized: bool
) -> None:
    original, compiled = factories
    if customized:
        original.set_parser_defaults(blob_parser=Mock(), timestamp_parser=Mock())
        compiled = _ParserFactory(original, _MODEL, compiled._parser_class)
    plans = compiled._plans
    factory = original if setter == "original" else compiled
    converter = MagicMock(side_effect=lambda value: ("updated", value))
    cast(MagicMock, converter.__eq__).side_effect = AssertionError(
        "compare defaults by identity"
    )
    factory.set_parser_defaults(blob_parser=converter, timestamp_parser=converter)
    for shape, response in [
        (
            _MODEL.operation_model("GetItem").output_shape,
            _response({"Item": {"b": {"B": "YQ=="}}}),
        ),
        (
            _MODEL.operation_model("DescribeTable").output_shape,
            _response({"Table": {"CreationDateTime": 1.25}}),
        ),
        (
            _MODEL.shape_for("ConditionalCheckFailedException"),
            _response(
                {"__type": "ConditionalCheckFailedException", "Item": _ITEM}, 400
            ),
        ),
        (_MODEL.shape_for("AttributeValue"), _response({"B": "YQ=="})),
    ]:
        stock = original.create_parser("json")
        parser = compiled.create_parser("json")
        _ordered_equal(
            await _parse(parser, response, shape),
            await _parse(stock, response, shape),
        )
        assert type(parser) is type(stock)
    assert compiled._plans is plans


def test_exceptions_are_not_retried() -> None:
    blob = Mock(side_effect=RuntimeError("custom failure"))
    original: Any = ResponseParserFactory()
    original.set_parser_defaults(blob_parser=blob)
    compiled = _ParserFactory(original, _MODEL, _CompiledJSONParser)
    with pytest.raises(RuntimeError, match="custom failure"):
        compiled.create_parser("json").parse(
            _response({"Item": {"b": {"B": "YQ=="}}}),
            _MODEL.operation_model("GetItem").output_shape,
        )
    blob.assert_called_once_with("YQ==")


def test_streaming_and_uncompiled_shapes_delegate() -> None:
    resolver: Any = cast(Any, ShapeResolver)(
        {
            "Root": {"type": "structure", "members": {"events": {"shape": "Events"}}},
            "Events": {"type": "structure", "eventstream": True, "members": {}},
        }
    )
    shape = resolver.get_shape_by_name("Root")
    model = Mock(operation_names=["Stream"])
    model.operation_model.return_value.output_shape = shape
    compiled = _ParserFactory(ResponseParserFactory(), model, _CompiledJSONParser)
    assert not compiled._plans
    parser = compiled.create_parser("json")
    assert parser._handle_json_body(b"{}", shape) == {}


def test_custom_factories_and_other_services_are_untouched() -> None:
    class CustomFactory(ResponseParserFactory):
        pass

    for model, original in [
        (_MODEL, CustomFactory()),
        (SimpleNamespace(service_name="s3"), ResponseParserFactory()),
        (
            SimpleNamespace(service_name="dynamodb", protocol="rest-json"),
            ResponseParserFactory(),
        ),
    ]:
        client = SimpleNamespace(
            meta=SimpleNamespace(service_model=model),
            _endpoint=SimpleNamespace(_response_parser_factory=original),
        )
        install_response_parser(client)
        assert client._endpoint._response_parser_factory is original


def test_custom_factory_descriptor_is_not_accessed() -> None:
    class CustomFactory:
        accesses = 0

        @property
        def create_parser(self) -> Any:  # noqa: ANN401 -- SDK parser factory method
            self.accesses += 1
            return ResponseParserFactory().create_parser

    original = CustomFactory()
    client = SimpleNamespace(
        meta=SimpleNamespace(service_model=_MODEL),
        _endpoint=SimpleNamespace(_response_parser_factory=original),
    )
    install_response_parser(client)
    assert client._endpoint._response_parser_factory is original
    assert original.accesses == 0


def test_sync_import_does_not_load_async_dependencies() -> None:
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import alternator.client, sys; assert not any(n.startswith(('aiobotocore', 'aiohttp')) for n in sys.modules)",
        ],
        check=True,
    )


@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
@pytest.mark.parametrize("kind", ["client", "resource", "async"])
async def test_real_factories_compression_signing_concurrency_and_hooks(
    fake_alternator_server: FakeAlternatorServer,
    contract_config: Config,
    encoding: str,
    kind: str,
) -> None:
    server = fake_alternator_server
    raw = json.dumps({"Item": _ITEM}).encode()
    compressed = gzip.compress(raw) if encoding == "gzip" else zlib.compress(raw)
    server.script(
        "GetItem",
        [
            FakeAlternatorResponse.json(
                {"__type": "InternalServerError", "message": "retry"}, status=500
            )
        ],
        fallback=FakeAlternatorResponse(
            200,
            compressed,
            {"Content-Encoding": encoding, "x-amzn-requestid": "compressed"},
        ),
    )
    config = replace(
        contract_config,
        retries=RetryConfig(max_attempts=2),
        response_compression=(ResponseCompression(encoding),),
    )
    auth = Auth.static_credentials("access", "secret")
    resource: Any = None
    if kind == "async":
        client: Any = await create_async_client(config, auth=auth)
    elif kind == "resource":
        resource = create_resource(config, auth=auth)
    else:
        client = create_client(config, auth=auth)
    try:
        if resource is not None:
            client = resource.meta.client
        factory = client._endpoint._response_parser_factory
        assert isinstance(factory, _ParserFactory)
        shape = client.meta.service_model.operation_model("GetItem").output_shape
        plan = factory._plans[shape]
        invoked = Event()

        def observe_plan(value: Any) -> Any:  # noqa: ANN401 -- SDK JSON value
            invoked.set()
            return plan(value)

        factory._plans = MappingProxyType(dict(factory._plans) | {shape: observe_plan})
        seen: list[dict[str, Any]] = []

        def hook(parsed: dict[str, Any], **kwargs: Any) -> None:  # noqa: ANN401 -- SDK events
            seen.append(parsed)
            parsed["hook"] = True

        client.meta.events.register("after-call.dynamodb.GetItem", hook)
        params = {"TableName": "contract", "Key": {"pk": {"S": "k"}}}
        if kind == "async":
            first = await client.get_item(**params)
            results = await asyncio.gather(
                *(client.get_item(**params) for _ in range(8))
            )
        elif resource is not None:
            table = resource.Table("contract")
            first = table.get_item(Key={"pk": "k"})
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(
                    pool.map(lambda _: table.get_item(Key={"pk": "k"}), range(8))
                )
        else:
            first = client.get_item(**params)
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda _: client.get_item(**params), range(8)))
        assert invoked.is_set()
        assert first["ResponseMetadata"]["RetryAttempts"] == 1
        assert first["ResponseMetadata"]["RequestId"] == "compressed"
        assert len(seen) == 9
        for result in [first, *results]:
            assert result["hook"] is True
            item = result["Item"]
            if resource is not None:
                assert item["binary"] == Binary(b"\x00\xff")
                assert item["set"] == {Binary(b"a"), Binary(b"b")}
                assert item["numbers"] == {Decimal("1.2"), Decimal("0")}
                assert item["strings"] == {"a", "b"}
                assert item["nested"] == {"list": [Decimal("1.25"), None, False]}
            else:
                assert item["binary"] == {"B": b"\x00\xff"}
                assert item["numbers"] == {"NS": ["1.2", "0"]}
        assert all(
            (r.header("Authorization") or "").startswith("AWS4-HMAC-SHA256 ")
            for r in server.captured_requests(operation="GetItem")
        )
    finally:
        if kind == "async":
            await close_async_client(client)
        elif resource is not None:
            close_resource(resource)
        else:
            close_client(client)


def test_clients_do_not_share_factory_or_plans(contract_config: Config) -> None:
    with ExitStack() as stack:
        stock: Any = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="x",
            aws_secret_access_key="x",
        )
        stack.callback(stock.close)
        original = stock._endpoint._response_parser_factory
        first: Any = create_client(contract_config)
        stack.callback(close_client, first)
        second: Any = create_client(contract_config)
        stack.callback(close_client, second)
        a, b = (
            first._endpoint._response_parser_factory,
            second._endpoint._response_parser_factory,
        )
        assert isinstance(a, _ParserFactory) and isinstance(b, _ParserFactory)
        assert a is not b and a._plans is not b._plans
        assert a._original is b._original is original
        assert stock._endpoint._response_parser_factory is original
        assert type(original.create_parser("json")) is JSONParser


async def test_async_clients_have_independent_factories(
    contract_config: Config,
) -> None:
    from aiobotocore.session import get_session

    session = get_session()
    async with session.create_client(
        "dynamodb",
        region_name="us-east-1",
        aws_access_key_id="x",
        aws_secret_access_key="x",
    ) as ordinary:
        original = cast(Any, ordinary)._endpoint._response_parser_factory
        first = await create_async_client(contract_config)
        try:
            second = await create_async_client(contract_config)
            try:
                a = cast(Any, first)._endpoint._response_parser_factory
                b = cast(Any, second)._endpoint._response_parser_factory
                assert isinstance(a, _ParserFactory) and isinstance(b, _ParserFactory)
                assert a is not b and a._plans is not b._plans
                assert (
                    cast(Any, ordinary)._endpoint._response_parser_factory is original
                )
                assert type(original) is AioResponseParserFactory
            finally:
                await close_async_client(second)
        finally:
            await close_async_client(first)


def test_custom_json_registration_is_honored(monkeypatch: pytest.MonkeyPatch) -> None:
    from botocore.parsers import PROTOCOL_PARSERS

    class CustomJSONParser(JSONParser):
        pass

    monkeypatch.setitem(PROTOCOL_PARSERS, "json", CustomJSONParser)
    original = ResponseParserFactory()
    client = SimpleNamespace(
        meta=SimpleNamespace(service_model=_MODEL),
        _endpoint=SimpleNamespace(_response_parser_factory=original),
    )
    install_response_parser(client)
    assert client._endpoint._response_parser_factory is original


@pytest.mark.parametrize("replacement", [False, True], ids=["entry", "mapping"])
async def test_late_json_registration_is_honored(
    factories: tuple[Any, _ParserFactory],
    monkeypatch: pytest.MonkeyPatch,
    replacement: bool,
) -> None:
    original, compiled = factories
    module = sys.modules[type(original).__module__]
    registry = module.PROTOCOL_PARSERS
    custom = type(
        "CustomJSONParser",
        (registry["json"],),
        {"_handle_string": lambda _self, _shape, value: "registered:" + value},
    )
    plans = compiled._plans
    responses = [
        (
            _MODEL.operation_model("GetItem").output_shape,
            _response({"Item": {"x": {"S": "leaf"}}}),
        ),
        (
            _MODEL.shape_for("ConditionalCheckFailedException"),
            _response(
                {
                    "__type": "ConditionalCheckFailedException",
                    "Message": "failure",
                    "Item": {"x": {"S": "leaf"}},
                },
                400,
            ),
        ),
    ]
    with monkeypatch.context() as patch:
        if replacement:
            patch.setattr(module, "PROTOCOL_PARSERS", dict(registry) | {"json": custom})
        else:
            patch.setitem(registry, "json", custom)
        for shape, response in responses:
            parser = compiled.create_parser("json")
            _ordered_equal(
                await _parse(parser, response, shape),
                await _parse(original.create_parser("json"), response, shape),
            )
            assert type(parser) is custom
    parser = compiled.create_parser("json")
    assert isinstance(parser, _CompiledJSONParser)
    assert parser._plans is plans
    for shape, response in responses:
        _ordered_equal(
            await _parse(parser, response, shape),
            await _parse(original.create_parser("json"), response, shape),
        )


async def test_class_factory_property_is_not_accessed(
    factories: tuple[Any, _ParserFactory], monkeypatch: pytest.MonkeyPatch
) -> None:
    original, compiled = factories
    stock_class = type(original.create_parser("json"))
    calls = Mock()

    def getter(_self: Any) -> Any:  # noqa: ANN401 -- SDK factory property
        calls()
        generation = calls.call_count
        return lambda _protocol: stock_class(
            blob_parser=lambda value: ("getter", generation, value)
        )

    monkeypatch.setattr(type(original), "create_parser", property(getter))
    client = SimpleNamespace(
        meta=SimpleNamespace(service_model=_MODEL),
        _endpoint=SimpleNamespace(_response_parser_factory=original),
    )
    install_response_parser(
        client, asynchronous=compiled._parser_class is not _CompiledJSONParser
    )
    calls.assert_not_called()
    assert client._endpoint._response_parser_factory is original
    parsed = await _parse(
        original.create_parser("json"),
        _response({"Item": {"x": {"B": "YQ=="}}}),
        _MODEL.operation_model("GetItem").output_shape,
    )
    assert parsed["Item"] == {"x": {"B": ("getter", 1, "YQ==")}}


async def test_custom_registration_constructor_is_not_called(
    factories: tuple[Any, _ParserFactory], monkeypatch: pytest.MonkeyPatch
) -> None:
    original, compiled = factories
    module = sys.modules[type(original).__module__]
    stock_class = module.PROTOCOL_PARSERS["json"]
    calls = Mock()

    def constructor(parser: Any, **kwargs: Any) -> None:  # noqa: ANN401 -- SDK parser defaults
        calls()
        generation = calls.call_count
        kwargs["blob_parser"] = lambda value: ("constructor", generation, value)
        stock_class.__init__(parser, **kwargs)

    custom = type("CustomJSONParser", (stock_class,), {"__init__": constructor})
    monkeypatch.setitem(module.PROTOCOL_PARSERS, "json", custom)
    client = SimpleNamespace(
        meta=SimpleNamespace(service_model=_MODEL),
        _endpoint=SimpleNamespace(_response_parser_factory=original),
    )
    install_response_parser(
        client, asynchronous=compiled._parser_class is not _CompiledJSONParser
    )
    calls.assert_not_called()
    assert client._endpoint._response_parser_factory is original
    parsed = await _parse(
        original.create_parser("json"),
        _response({"Item": {"x": {"B": "YQ=="}}}),
        _MODEL.operation_model("GetItem").output_shape,
    )
    assert parsed["Item"] == {"x": {"B": ("constructor", 1, "YQ==")}}


@pytest.mark.parametrize("initial", [False, True], ids=["late", "initial"])
async def test_opaque_factory_callable_is_not_inspected(
    factories: tuple[Any, _ParserFactory], initial: bool
) -> None:
    original, compiled = factories
    stock_class = type(original.create_parser("json"))

    class OpaqueFactory:
        def __getattribute__(self, name: str) -> Any:  # noqa: ANN401 -- opaque callable metadata
            if name in {"__func__", "__self__", "__globals__"}:
                raise AssertionError("opaque callable metadata inspected")
            return object.__getattribute__(self, name)

        def __call__(self, _protocol: str) -> Any:  # noqa: ANN401 -- SDK parser
            return stock_class(blob_parser=lambda value: ("opaque", value))

    original.create_parser = OpaqueFactory()
    factory = compiled
    if initial:
        client = SimpleNamespace(
            meta=SimpleNamespace(service_model=_MODEL),
            _endpoint=SimpleNamespace(_response_parser_factory=original),
        )
        install_response_parser(
            client, asynchronous=compiled._parser_class is not _CompiledJSONParser
        )
        factory = client._endpoint._response_parser_factory
        assert factory is original
    response = _response({"Item": {"x": {"B": "YQ=="}}})
    shape = _MODEL.operation_model("GetItem").output_shape
    _ordered_equal(
        await _parse(factory.create_parser("json"), response, shape),
        await _parse(original.create_parser("json"), response, shape),
    )


@pytest.mark.parametrize("rebound", [False, True], ids=["custom", "rebound"])
async def test_late_factory_method_customization_is_honored(
    factories: tuple[Any, _ParserFactory], rebound: bool
) -> None:
    original, compiled = factories
    create_parser = original.create_parser
    stock_class = type(create_parser("json"))
    plans = compiled._plans
    shape = _MODEL.operation_model("GetItem").output_shape
    response = _response({"Item": {"x": {"B": "YQ=="}}})
    if rebound:
        other = type(original)()
        other.set_parser_defaults(blob_parser=lambda value: ("custom", value))
        original.create_parser = other.create_parser
    else:
        original.create_parser = Mock(
            side_effect=lambda _protocol: stock_class(
                blob_parser=lambda value: ("custom", value)
            )
        )
    expected = await _parse(original.create_parser("json"), response, shape)
    if not rebound:
        original.create_parser.reset_mock()
    _ordered_equal(
        await _parse(compiled.create_parser("json"), response, shape), expected
    )
    if not rebound:
        original.create_parser.assert_called_once_with("json")
    original.create_parser = create_parser
    parser = compiled.create_parser("json")
    assert isinstance(parser, _CompiledJSONParser)
    assert parser._plans is plans


@pytest.mark.parametrize("wrapped", [False, True], ids=["raw", "wrapped"])
async def test_class_customized_factory_is_untouched(
    factories: tuple[Any, _ParserFactory],
    monkeypatch: pytest.MonkeyPatch,
    wrapped: bool,
) -> None:
    original, compiled = factories
    factory_class = type(original)
    stock_class = type(original.create_parser("json"))
    calls = Mock()

    def override(_self: Any, protocol: str) -> Any:  # noqa: ANN401 -- SDK factory method
        calls(protocol)
        return stock_class(blob_parser=lambda value: ("class", value))

    method = wraps(factory_class.create_parser)(override) if wrapped else override
    client = SimpleNamespace(
        meta=SimpleNamespace(service_model=_MODEL),
        _endpoint=SimpleNamespace(_response_parser_factory=original),
    )
    with monkeypatch.context() as patch:
        patch.setattr(factory_class, "create_parser", method)
        install_response_parser(
            client, asynchronous=compiled._parser_class is not _CompiledJSONParser
        )
        calls.assert_not_called()
        assert client._endpoint._response_parser_factory is original
        shape = _MODEL.operation_model("GetItem").output_shape
        response = _response({"Item": {"x": {"B": "YQ=="}}})
        parsed = await _parse(
            client._endpoint._response_parser_factory.create_parser("json"),
            response,
            shape,
        )
        assert parsed["Item"] == {"x": {"B": ("class", "YQ==")}}
    install_response_parser(
        client, asynchronous=compiled._parser_class is not _CompiledJSONParser
    )
    assert isinstance(client._endpoint._response_parser_factory, _ParserFactory)


@pytest.mark.parametrize("rebound", [False, True], ids=["custom", "rebound"])
def test_instance_customized_factory_is_untouched(rebound: bool) -> None:
    original: Any = ResponseParserFactory()
    if rebound:
        other = ResponseParserFactory()
        other.set_parser_defaults(blob_parser=lambda value: value)
        original.create_parser = other.create_parser
    else:
        original.create_parser = Mock(
            return_value=JSONParser(blob_parser=lambda value: value)
        )
    client = SimpleNamespace(
        meta=SimpleNamespace(service_model=_MODEL),
        _endpoint=SimpleNamespace(_response_parser_factory=original),
    )
    install_response_parser(client)
    assert client._endpoint._response_parser_factory is original
    if not rebound:
        original.create_parser.assert_not_called()
