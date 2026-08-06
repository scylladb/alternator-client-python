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
        return "aws_access_key_id" in boto_kwargs

    resolved_auth = auth or Auth.disabled()
    if not resolved_auth.enabled:
        return False

    boto_kwargs.update(resolved_auth.as_boto_kwargs())
    return True
