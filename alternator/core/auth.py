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

"""Authentication helpers for Alternator boto clients."""

from __future__ import annotations

import warnings
from typing import Any

from alternator.config import Auth
from alternator.exceptions import ConfigurationError

_CREDENTIAL_KWARGS = frozenset(
    {
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
    }
)


def apply_auth(auth: Auth | None, boto_kwargs: dict[str, Any]) -> bool:
    """
    Apply explicit Alternator auth to boto kwargs.

    Returns:
        Whether request signing should be enabled.
    """
    legacy_credential_keys = _CREDENTIAL_KWARGS.intersection(boto_kwargs)
    if legacy_credential_keys:
        if auth is not None:
            raise ConfigurationError(
                "Do not combine auth=... with raw boto credential kwargs; "
                "use Auth.static_credentials(...) instead"
            )
        warnings.warn(
            "Passing raw boto credential kwargs is deprecated; "
            "use auth=Auth.static_credentials(...) instead.",
            DeprecationWarning,
            stacklevel=3,
        )

        if all(boto_kwargs[key] is None for key in legacy_credential_keys):
            for key in legacy_credential_keys:
                del boto_kwargs[key]
            return False

        access_key_id = boto_kwargs.get("aws_access_key_id")
        secret_access_key = boto_kwargs.get("aws_secret_access_key")
        if not isinstance(access_key_id, str) or not access_key_id:
            raise ConfigurationError(
                "Raw boto credentials require a non-empty aws_access_key_id"
            )
        if not isinstance(secret_access_key, str) or not secret_access_key:
            raise ConfigurationError(
                "Raw boto credentials require a non-empty aws_secret_access_key"
            )

        session_token = boto_kwargs.get("aws_session_token")
        if session_token is not None and (
            not isinstance(session_token, str) or not session_token
        ):
            raise ConfigurationError(
                "Raw boto credentials require aws_session_token to be a non-empty "
                "string when provided"
            )
        return True

    resolved_auth = auth or Auth.disabled()
    if not resolved_auth.enabled:
        return False

    boto_kwargs.update(resolved_auth.as_boto_kwargs())
    return True
