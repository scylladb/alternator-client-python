# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Reserved error classes from the installed extension and Python translation."""

from __future__ import annotations

from unittest.mock import patch

import _alternator_rust as native
from botocore.exceptions import ClientError, ParamValidationError, SSLError

from alternator._experimental.rust._errors import adapt_error


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

        transport = native.TransportError("TLS failed")
        transport.category = "tls"
        transport.endpoint_url = "https://node:8043"
        result = adapt_error(transport, native)
        assert isinstance(result, SSLError)
        assert transport.endpoint_url in str(result)
