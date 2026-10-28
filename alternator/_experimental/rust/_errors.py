# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Translate the private native exceptions without constructing an SDK client."""

from __future__ import annotations

from functools import lru_cache
from threading import Lock
from types import ModuleType
from typing import cast

from botocore import exceptions
from botocore.errorfactory import BaseClientExceptions, ClientExceptionsFactory
from botocore.session import get_session

_model_lock = Lock()


def service_exceptions() -> BaseClientExceptions:
    """Share one class registry, including concurrent first access."""
    with _model_lock:
        return _service_exceptions()


@lru_cache(maxsize=1)
def _service_exceptions() -> BaseClientExceptions:
    """Load the cached service model; no credentials, endpoint or admin client."""
    model = get_session().get_service_model("dynamodb")
    return ClientExceptionsFactory().create_client_exceptions(model)


def adapt_error(error: Exception, native: ModuleType) -> Exception:
    """Return a botocore-compatible exception, retaining native wire context.

    The facade passes its already version-checked extension. Runtime/closed-client
    errors keep their original identity. This function never loads the extension.
    """
    if isinstance(error, native.ValidationError):
        return exceptions.ParamValidationError(report=error.report)
    if isinstance(error, native.ServiceError):
        response = error.response
        exception_type = cast(
            type[exceptions.ClientError],
            service_exceptions().from_code(response["Error"]["Code"]),
        )
        try:
            translated = exception_type(response, error.operation_name)
        except ValueError as cause:
            # The cached factory creates bare ClientError subclasses. Do not
            # bypass a replacement class with other construction/formatting.
            bare_factory_class = exception_type is exceptions.ClientError or (
                exception_type.__bases__ == (exceptions.ClientError,)
                and set(vars(exception_type)) <= {"__module__", "__doc__"}
            )
            if (
                not bare_factory_class
                or exception_type.__init__ is not exceptions.ClientError.__init__
                or exception_type.__new__ is not Exception.__new__
            ):
                raise
            translated = Exception.__new__(exception_type)
            Exception.__init__(
                translated,
                f"An error occurred ({response['Error']['Code']}) when calling "
                f"the {error.operation_name} operation: <Error.Message rendering failed: ValueError>",
            )
            translated.response = response
            translated.operation_name = error.operation_name
            translated.__cause__ = cause
        translated.__dict__["body"] = error.body
        translated.__dict__["headers"] = error.headers
        return translated
    if isinstance(error, native.TransportError):
        category = error.category
        endpoint = error.endpoint_url
        if category == "endpoint_connection":
            return exceptions.EndpointConnectionError(
                endpoint_url=endpoint, error=error
            )
        if category == "connection_closed":
            return exceptions.ConnectionClosedError(endpoint_url=endpoint, error=error)
        if category == "connect_timeout":
            return exceptions.ConnectTimeoutError(endpoint_url=endpoint, error=error)
        if category == "read_timeout":
            return exceptions.ReadTimeoutError(endpoint_url=endpoint, error=error)
        if category == "tls":
            return exceptions.SSLError(endpoint_url=endpoint, error=error)
    return error
