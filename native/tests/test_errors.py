# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Reserved error classes from the installed extension and Python translation."""

from __future__ import annotations

import sys
import traceback
from unittest.mock import patch

import _alternator_rust as native
from botocore.exceptions import ClientError, ParamValidationError, SSLError

from alternator._experimental.rust._errors import adapt_error, service_exceptions


def test_installed_native_error_interfaces() -> None:
    assert native.INTERFACE_VERSION == 1
    for cls in (native.ValidationError, native.ServiceError, native.TransportError):
        assert issubclass(cls, Exception)
        assert cls.__module__ == "_alternator_rust"
    with patch("botocore.session.Session.create_client", side_effect=AssertionError):
        validation = native.ValidationError("invalid parameter")
        validation.report = "PutItem.Item: expected dict"
        result = adapt_error(validation, native)
        assert isinstance(result, ParamValidationError)
        assert validation.report in str(result)

        service = native.ServiceError("service failure")
        service.response = {
            "Error": {"Code": "ConditionalCheckFailedException", "Message": "failed"},
            "Item": {"pk": {"B": b"\x00\xff"}},
            "message": "failed",
            "future": [],
            "ResponseMetadata": {"HTTPStatusCode": 400, "RetryAttempts": 2},
        }
        service.operation_name = "PutItem"
        service.body = b"original wire bytes"
        service.headers = [("x-amzn-requestid", b"request-id")]
        result = adapt_error(service, native)
        assert isinstance(result, ClientError)
        assert type(result).__name__ == "ConditionalCheckFailedException"
        assert result.response is service.response
        assert result.operation_name == "PutItem"
        assert result.body == service.body
        assert result.headers == service.headers

        limit = sys.get_int_max_str_digits()
        print("installed adapter startup digit limit:", limit)
        number = 10**5000 - 1
        message = {"number": number}
        retained = {"number": number}
        service.response["Error"]["Message"] = message
        service.response["message"] = retained
        service.body = b'{"message":{"number":' + b"9" * 5000 + b"}}"
        result = adapt_error(service, native)
        assert type(result) is service_exceptions().from_code(
            "ConditionalCheckFailedException"
        )
        assert result.response is service.response
        assert result.response["Error"]["Message"] is message
        assert result.operation_name is service.operation_name
        assert result.body is service.body and result.headers is service.headers
        if 0 < limit < 5000:
            assert isinstance(result.__cause__, ValueError)
            assert "Error.Message rendering failed" in str(result)
            assert "Error.Message rendering failed" in repr(result)
            assert "Error.Message rendering failed" in "".join(
                traceback.format_exception(result)
            )
        else:
            assert result.__cause__ is None
            assert (
                result.args
                == type(result)(service.response, service.operation_name).args
            )
        retained["number"] = 0
        assert message["number"] == number
        assert sys.get_int_max_str_digits() == limit

        transport = native.TransportError("TLS failed")
        transport.category = "tls"
        transport.endpoint_url = "https://node:8043"
        result = adapt_error(transport, native)
        assert isinstance(result, SSLError)
        assert transport.endpoint_url in str(result)
