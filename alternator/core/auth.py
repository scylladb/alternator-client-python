"""Authentication helpers for Alternator boto clients."""

from __future__ import annotations

from typing import Any

from alternator.config import Auth
from alternator.exceptions import ConfigurationError

_BOTO_CREDENTIAL_KWARGS = frozenset(
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
    boto_credential_keys = _BOTO_CREDENTIAL_KWARGS.intersection(boto_kwargs)
    if boto_credential_keys:
        if auth is not None:
            raise ConfigurationError(
                "Do not combine auth=... with raw boto credential kwargs; "
                "use Auth.static_credentials(...) instead"
            )
        return "aws_access_key_id" in boto_kwargs

    resolved_auth = auth or Auth.disabled()
    if not resolved_auth.enabled:
        return False

    boto_kwargs.update(resolved_auth.as_boto_kwargs())
    return True
