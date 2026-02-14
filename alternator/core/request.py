"""Request parsing utilities for Alternator client."""

from __future__ import annotations

import json
from typing import Any

_PARSED_PARAMS_ATTR = "_alternator_parsed_params"


def extract_operation_name(request: Any) -> str:
    """
    Extract operation name from the X-Amz-Target header.

    Args:
        request: botocore request object

    Returns:
        Operation name (e.g. "PutItem"), or empty string if not found
    """
    target = request.headers.get("X-Amz-Target", "")
    if isinstance(target, bytes):
        target = target.decode("utf-8")
    return target.split(".")[-1] if "." in target else ""


def extract_request_params(request: Any) -> dict[str, Any]:
    """
    Parse the request body to extract parameters.

    Caches the parsed result on the request object to avoid
    re-parsing on subsequent calls for the same request.

    Args:
        request: botocore request object

    Returns:
        Parsed request parameters dict
    """
    try:
        cached = request.__dict__[_PARSED_PARAMS_ATTR]
        return cached
    except (KeyError, AttributeError):
        pass

    result: dict[str, Any] = {}
    if request.body:
        try:
            body = request.body
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            result = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    setattr(request, _PARSED_PARAMS_ATTR, result)
    return result
