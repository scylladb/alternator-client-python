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

"""Endpoint-local compilation of DynamoDB JSON success response shapes.

Only shape traversal is replaced. JSON decoding, errors, metadata and event
streams remain SDK responsibilities. Plans are built once and never extended
while requests are running; each response still gets its own SDK parser.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from inspect import getattr_static
from types import FunctionType, MappingProxyType, MethodType
from typing import Any, cast

from botocore import parsers as botocore_parsers
from botocore.model import ServiceModel, Shape
from botocore.parsers import JSONParser, ResponseParserFactory

# SDK JSON values and parser internals are intentionally dynamic.
_Parse = Callable[[Any], Any]


class _Compiler:
    def __init__(self, parser: Any) -> None:  # noqa: ANN401 -- SDK private shape handlers
        self._parser = parser
        self._cache: dict[tuple[str, str], _Parse] = {}

    def compile(self, shape: Any) -> _Parse:  # noqa: ANN401 -- SDK private shape traits
        # A reference may override any shape trait, including members/document/
        # union, so a name alone is not a sufficient cache key. The raw model
        # contains references rather than expanded shapes, including recursion.
        # Keep member order and include effective, potentially overridden traits.
        key = (
            shape.name,
            json.dumps((shape._shape_model, shape.serialization, shape.metadata)),
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        target: list[_Parse] = []

        def forward(value: Any) -> Any:  # noqa: ANN401 -- SDK JSON value
            return target[0](value)

        self._cache[key] = forward

        def stock(value: Any) -> Any:  # noqa: ANN401 -- SDK JSON value
            return self._parser._parse_shape(shape, value)

        kind = shape.type_name
        handler: _Parse = stock
        if kind == "structure" and not (
            shape.is_document_type or shape.is_tagged_union or shape.event_stream_name
        ):
            fields = {
                member.serialization.get("name", name): (
                    index,
                    name,
                    self.compile(member),
                )
                for index, (name, member) in enumerate(shape.members.items())
            }

            def structure(value: Any) -> Any:  # noqa: ANN401 -- SDK JSON value
                if value is None:
                    return None
                if not isinstance(value, dict):
                    return stock(value)
                present = []
                for name, raw in value.items():
                    field = fields.get(name)
                    if field is not None and raw is not None:
                        present.append((field, raw))
                # The SDK emits structure members in model order, even when the
                # wire order differs. Maps and lists keep their wire order.
                if len(present) > 1:
                    present.sort(key=lambda entry: entry[0][0])
                # A comprehension adds a frame per recursive attribute on
                # Python 3.10/3.11, exhausting the stack before stock parsing.
                result = {}
                for field, raw in present:
                    result[field[1]] = field[2](raw)
                return result

            # Aliased fields can share one wire name. Preserve stock handling
            # of this unusual model instead of losing one of those fields.
            if len(fields) == len(shape.members):
                handler = structure
        elif kind == "map":
            parse_key = self.compile(shape.key)
            parse_value = self.compile(shape.value)

            def mapping(value: Any) -> Any:  # noqa: ANN401 -- SDK JSON value
                if not isinstance(value, dict):
                    return stock(value)
                return {parse_key(k): parse_value(v) for k, v in value.items()}

            handler = mapping
        elif kind == "list":
            parse_member = self.compile(shape.member)

            def sequence(value: Any) -> Any:  # noqa: ANN401 -- SDK JSON value
                if not isinstance(value, list):
                    return stock(value)
                return [None if v is None else parse_member(v) for v in value]

            handler = sequence
        elif kind == "blob":
            handler = self._parser._blob_parser
        elif kind == "timestamp":
            handler = self._parser._timestamp_parser
        elif kind in {"string", "integer", "long", "float", "double", "boolean"}:

            def scalar(value: Any) -> Any:  # noqa: ANN401 -- SDK JSON value
                return value

            handler = scalar
        target.append(handler)
        self._cache[key] = handler
        return handler


class _CompiledJSONParser(JSONParser):
    _plans: Mapping[Shape, _Parse]
    _parser_module = botocore_parsers
    _stock_parser_class: type[JSONParser] = JSONParser

    def _handle_json_body(self, raw_body: bytes, shape: Shape) -> Any:  # noqa: ANN401 -- SDK parsed value
        plan = self._plans.get(shape)
        if plan is None:
            # In particular, modeled errors and streaming responses are never
            # compiled, even when their nested shapes overlap success shapes.
            return cast(Any, super())._handle_json_body(raw_body, shape)
        return plan(cast(Any, self)._parse_body_as_json(raw_body))


def _async_parser_class() -> type[_CompiledJSONParser]:
    # Keep the optional dependency entirely off the synchronous import path.
    from aiobotocore import parsers as aio_parsers

    class _CompiledAioJSONParser(_CompiledJSONParser, aio_parsers.AioJSONParser):
        _parser_module = aio_parsers
        _stock_parser_class = aio_parsers.AioJSONParser

    return _CompiledAioJSONParser


class _ParserFactory:
    def __init__(
        self,
        original: Any,  # noqa: ANN401 -- SDK private factory defaults
        model: ServiceModel,
        parser_class: type[_CompiledJSONParser],
    ) -> None:
        self._original = original
        self._create_parser = original.create_parser
        self._defaults = dict(original._defaults)
        self._parser_class = parser_class
        parser = JSONParser(**self._defaults)
        self._timestamp_parser = cast(Any, parser)._timestamp_parser
        compiler = _Compiler(parser)
        plans: dict[Shape, _Parse] = {}
        for name in model.operation_names:
            shape = model.operation_model(name).output_shape
            if shape is not None and not (
                shape.event_stream_name or shape.serialization.get("streaming")
            ):
                plans[shape] = compiler.compile(shape)
        self._plans = MappingProxyType(plans)

    def set_parser_defaults(self, **kwargs: Any) -> None:  # noqa: ANN401 -- SDK parser defaults
        self._original.set_parser_defaults(**kwargs)

    def create_parser(self, protocol_name: str) -> Any:  # noqa: ANN401 -- SDK protocol parser
        create_parser = self._original.create_parser
        # Registrations and factory methods can change after installation too.
        # Keep the module reference so replacing its registry remains visible.
        if (
            protocol_name != "json"
            or type(create_parser) is not MethodType
            or create_parser.__func__ is not self._create_parser.__func__
            or create_parser.__self__ is not self._create_parser.__self__
            or self._parser_class._parser_module.PROTOCOL_PARSERS.get("json")
            is not self._parser_class._stock_parser_class
        ):
            return create_parser(protocol_name)
        # SDK sessions can share the original factory and update its defaults.
        # Existing plans retain their converters, including the SDK's implicit
        # timestamp default. Use stock parsing after an update rather than
        # rebuilding plans or comparing arbitrary callables.
        defaults = dict(self._original._defaults)
        if (
            defaults.keys() != self._defaults.keys()
            or any(
                defaults[name] is not value for name, value in self._defaults.items()
            )
            or (
                self._defaults.get("timestamp_parser") is None
                and botocore_parsers.DEFAULT_TIMESTAMP_PARSER
                is not self._timestamp_parser
            )
        ):
            return create_parser(protocol_name)
        parser = self._parser_class(**self._defaults)
        parser._plans = self._plans
        return parser


def install_response_parser(client: Any, *, asynchronous: bool = False) -> None:  # noqa: ANN401 -- private SDK client interface
    """Replace only this endpoint's stock DynamoDB JSON parser factory."""
    model = client.meta.service_model
    if model.service_name != "dynamodb" or model.protocol != "json":
        return
    original = client._endpoint._response_parser_factory
    factory_class = ResponseParserFactory
    parser_class: type[_CompiledJSONParser] = _CompiledJSONParser
    if asynchronous:
        from aiobotocore.parsers import AioResponseParserFactory

        factory_class = AioResponseParserFactory
        parser_class = _async_parser_class()
    # Subclasses and custom factories may implement their own parsing contract.
    if type(original) is not factory_class:
        return
    function = getattr_static(original, "create_parser", None)
    # Inspect before binding so custom descriptors and parser constructors are
    # never invoked merely to decide whether installation is appropriate.
    # Class-level wrappers can copy SDK names/module metadata via wraps(), but
    # their function globals still belong to the application, not this SDK.
    if (
        type(function) is not FunctionType
        or function.__globals__ is not parser_class._parser_module.__dict__
        or function is not getattr_static(factory_class, "create_parser")
        or parser_class._parser_module.PROTOCOL_PARSERS.get("json")
        is not parser_class._stock_parser_class
    ):
        return
    create_parser = original.create_parser
    if (
        type(create_parser) is not MethodType
        or create_parser.__func__ is not function
        or create_parser.__self__ is not original
    ):
        return
    client._endpoint._response_parser_factory = _ParserFactory(
        original, model, parser_class
    )
