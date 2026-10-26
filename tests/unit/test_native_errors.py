# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""The private adapter builds modeled exceptions without a Python client."""

from __future__ import annotations

from types import ModuleType
from typing import Any
from unittest.mock import patch

import pytest
from botocore import exceptions
from botocore.errorfactory import BaseClientExceptions

from alternator._experimental.rust._errors import (
    _service_exceptions,
    adapt_error,
    service_exceptions,
)


class ValidationError(Exception):
    report = 'PutItem.Item["bad"].S: expected str'


class ServiceError(Exception):
    def __init__(self, code: str) -> None:
        self.response: dict[str, Any] = {
            "Error": {"Code": code, "Message": "failure"},
            "Item": {"b": {"B": b"\x00\xff"}},
            "message": "failure",
            "future": {"data": []},
            "ResponseMetadata": {"RetryAttempts": 3},
        }
        self.operation_name = "PutItem"
        self.body = b"original compressed body"
        self.headers = [("content-encoding", b"gzip")]


class TransportError(Exception):
    def __init__(self, category: str) -> None:
        self.category = category
        self.endpoint_url = "https://node:8043"


def extension() -> ModuleType:
    native = ModuleType("test_native_errors")
    native.__dict__.update(
        ValidationError=ValidationError,
        ServiceError=ServiceError,
        TransportError=TransportError,
    )
    return native


@pytest.mark.parametrize(
    "code", ["ConditionalCheckFailedException", "UnknownFutureException"]
)
def test_service_error_without_python_client_or_serialization(code: str) -> None:
    error = ServiceError(code)
    _service_exceptions.cache_clear()
    with (
        patch("boto3.client", side_effect=AssertionError("created boto client")),
        patch(
            "botocore.session.Session.create_client",
            side_effect=AssertionError("created SDK client"),
        ),
        patch(
            "botocore.serialize.JSONSerializer.serialize_to_request",
            side_effect=AssertionError("serialized request"),
        ),
        patch(
            "botocore.validate.validate_parameters",
            side_effect=AssertionError("validated request"),
        ),
        patch(
            "botocore.httpsession.URLLib3Session.send",
            side_effect=AssertionError("Python HTTP"),
        ),
    ):
        translated = adapt_error(error, extension())
        assert isinstance(translated, exceptions.ClientError)
        assert translated.response is error.response
        assert translated.operation_name == "PutItem"
        assert translated.__dict__["body"] is error.body
        assert translated.__dict__["headers"] is error.headers
        assert service_exceptions() is service_exceptions()
    if code == "UnknownFutureException":
        assert type(translated) is exceptions.ClientError
    else:
        assert type(translated).__name__ == code


def test_validation_and_runtime_errors() -> None:
    error = ValidationError()
    translated = adapt_error(error, extension())
    assert isinstance(translated, exceptions.ParamValidationError)
    assert error.report in str(translated)
    runtime = RuntimeError("native client is closed")
    assert adapt_error(runtime, extension()) is runtime
    unknown = TransportError("future-category")
    assert adapt_error(unknown, extension()) is unknown


@pytest.mark.parametrize(
    ("category", "expected"),
    [
        ("endpoint_connection", exceptions.EndpointConnectionError),
        ("connection_closed", exceptions.ConnectionClosedError),
        ("connect_timeout", exceptions.ConnectTimeoutError),
        ("read_timeout", exceptions.ReadTimeoutError),
        ("tls", exceptions.SSLError),
    ],
)
def test_transport_error(category: str, expected: type[Exception]) -> None:
    error = TransportError(category)
    translated = adapt_error(error, extension())
    assert isinstance(translated, expected)
    assert error.endpoint_url in str(translated)


def test_concurrent_first_access_uses_one_modeled_class_registry() -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    _service_exceptions.cache_clear()
    barrier = Barrier(8)

    def lookup() -> BaseClientExceptions:
        barrier.wait(timeout=5)
        return service_exceptions()

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: lookup(), range(8)))
    assert all(result is results[0] for result in results)
