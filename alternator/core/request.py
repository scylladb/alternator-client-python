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

"""Request parsing utilities for Alternator client."""

from __future__ import annotations

import json
from typing import Any

from botocore.awsrequest import AWSPreparedRequest, AWSRequest

_PARSED_PARAMS_ATTR = "_alternator_parsed_params"


def extract_operation_name(request: AWSPreparedRequest | AWSRequest) -> str:
    """
    Extract operation name from the X-Amz-Target header.

    Args:
        request: botocore request object

    Returns:
        Operation name (e.g. "PutItem"), or empty string if not found
    """
    target: str | bytes = request.headers.get("X-Amz-Target", "")
    if isinstance(target, bytes):
        target = target.decode("utf-8")
    return target.split(".")[-1] if "." in target else ""


def extract_request_params(request: AWSPreparedRequest | AWSRequest) -> dict[str, Any]:
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
        cached: dict[str, Any] = request.__dict__[_PARSED_PARAMS_ATTR]
        return cached
    except (KeyError, AttributeError):
        pass

    result: dict[str, Any] = {}
    if request.body:
        try:
            body = request.body
            if isinstance(body, (bytes, bytearray)):
                body = body.decode("utf-8")
            if isinstance(body, str):
                result = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    setattr(request, _PARSED_PARAMS_ATTR, result)
    return result
