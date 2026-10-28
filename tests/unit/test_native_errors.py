# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""The private adapter builds modeled exceptions without a Python client."""

from __future__ import annotations

import sys
import traceback
from types import ModuleType
from typing import Any, cast
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
        assert (
            translated.args
            == type(translated)(error.response, error.operation_name).args
        )
        assert service_exceptions() is service_exceptions()
    if code == "UnknownFutureException":
        assert type(translated) is exceptions.ClientError
    else:
        assert type(translated).__name__ == code


@pytest.mark.parametrize(
    "code", ["ConditionalCheckFailedException", "UnknownFutureException"]
)
@pytest.mark.parametrize("sign", [1, -1])
def test_service_error_preserves_unrenderable_message(code: str, sign: int) -> None:
    limit = sys.get_int_max_str_digits()
    print("adapter startup digit limit:", limit)
    number = sign * (10**5000 - 1)
    error = ServiceError(code)
    message = {"number": number}
    retained = {"number": number}
    error.response["Error"]["Message"] = message
    error.response["message"] = retained
    error.body = (
        b'{"message":{"number":' + (b"-" if sign < 0 else b"") + b"9" * 5000 + b"}}"
    )
    exception_type = cast(
        type[exceptions.ClientError], service_exceptions().from_code(code)
    )
    # The cached factory creates bare ClientError subclasses.
    assert exception_type.__init__ is exceptions.ClientError.__init__
    assert exception_type.__new__ is Exception.__new__
    translated = adapt_error(error, extension())
    assert isinstance(translated, exceptions.ClientError)
    assert type(translated) is exception_type
    # Native retained payloads may contain non-string messages, unlike SDK stubs.
    response = cast(dict[str, Any], translated.response)
    assert response is error.response
    assert response["Error"]["Message"] is message
    assert translated.operation_name is error.operation_name
    assert translated.__dict__["body"] is error.body
    assert translated.__dict__["headers"] is error.headers
    assert type(message["number"]) is int and message["number"] == number
    if 0 < limit < 5000:
        assert isinstance(translated.__cause__, ValueError)
        for rendered in (
            str(translated),
            repr(translated),
            "".join(traceback.format_exception(translated)),
        ):
            assert code in rendered and error.operation_name in rendered
            assert "Error.Message rendering failed" in rendered
        assert translated.args and isinstance(translated.args[0], str)
    else:
        assert translated.__cause__ is None
        assert (
            translated.args
            == exception_type(translated.response, error.operation_name).args
        )
    retained["number"] = 0
    assert message["number"] == number
    assert sys.get_int_max_str_digits() == limit


@pytest.mark.parametrize(
    "failure", [ValueError("custom constructor"), RuntimeError("unrelated")]
)
def test_replacement_constructor_failure_propagates(failure: Exception) -> None:
    class CustomClientError(exceptions.ClientError):
        def __init__(self, response: dict[str, Any], operation_name: str) -> None:
            raise failure

    code = "CustomError"
    with (
        patch(
            "alternator._experimental.rust._errors.service_exceptions",
            return_value=BaseClientExceptions({code: CustomClientError}),
        ),
        pytest.raises(type(failure)) as caught,
    ):
        adapt_error(ServiceError(code), extension())
    assert caught.value is failure


def test_custom_message_template_failure_propagates() -> None:
    class CustomClientError(exceptions.ClientError):
        MSG_TEMPLATE = "{error_code:invalid}"

    code = "CustomError"
    with (
        patch(
            "alternator._experimental.rust._errors.service_exceptions",
            return_value=BaseClientExceptions({code: CustomClientError}),
        ),
        pytest.raises(ValueError),
    ):
        adapt_error(ServiceError(code), extension())


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
