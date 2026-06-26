"""Tests for explicit Alternator auth configuration."""

import pytest

from alternator.config import Auth
from alternator.core.auth import apply_auth
from alternator.exceptions import ConfigurationError


class TestAuth:
    """Tests for Auth factory methods and validation."""

    def test_disabled_auth(self) -> None:
        """Disabled auth has no boto credential kwargs."""
        auth = Auth.disabled()

        assert auth.enabled is False
        assert auth.as_boto_kwargs() == {}

    def test_static_credentials(self) -> None:
        """Static credentials map to boto credential kwargs."""
        auth = Auth.static_credentials("alternator", "secret")

        assert auth.enabled is True
        assert auth.as_boto_kwargs() == {
            "aws_access_key_id": "alternator",
            "aws_secret_access_key": "secret",
        }

    def test_static_credentials_with_session_token(self) -> None:
        """Static credentials can include a session token."""
        auth = Auth.static_credentials("alternator", "secret", "token")

        assert auth.as_boto_kwargs()["aws_session_token"] == "token"

    def test_rejects_partial_static_credentials(self) -> None:
        """Static auth requires both key id and secret."""
        with pytest.raises(ConfigurationError, match="requires both"):
            Auth(access_key_id="alternator")

    def test_rejects_empty_static_credentials(self) -> None:
        """Static auth credentials cannot be empty strings."""
        with pytest.raises(ConfigurationError, match="must not be empty"):
            Auth.static_credentials("", "secret")


class TestApplyAuth:
    """Tests for applying auth to boto kwargs."""

    def test_default_auth_is_disabled(self) -> None:
        """No auth argument leaves signing disabled."""
        boto_kwargs: dict[str, object] = {}

        assert apply_auth(None, boto_kwargs) is False
        assert boto_kwargs == {}

    def test_disabled_auth_is_unsigned(self) -> None:
        """Explicit disabled auth leaves signing disabled."""
        boto_kwargs: dict[str, object] = {}

        assert apply_auth(Auth.disabled(), boto_kwargs) is False
        assert boto_kwargs == {}

    def test_static_auth_updates_boto_kwargs(self) -> None:
        """Static auth enables signing and injects credentials."""
        boto_kwargs: dict[str, object] = {"region_name": "us-east-1"}

        assert (
            apply_auth(Auth.static_credentials("alternator", "secret"), boto_kwargs)
            is True
        )
        assert boto_kwargs["aws_access_key_id"] == "alternator"
        assert boto_kwargs["aws_secret_access_key"] == "secret"
        assert boto_kwargs["region_name"] == "us-east-1"

    def test_legacy_boto_credentials_warn_and_enable_auth(self) -> None:
        """Raw boto credential kwargs still work but are deprecated."""
        boto_kwargs: dict[str, object] = {
            "aws_access_key_id": "alternator",
            "aws_secret_access_key": "secret",
        }

        with pytest.warns(DeprecationWarning, match="raw boto credential kwargs"):
            auth_enabled = apply_auth(None, boto_kwargs)

        assert auth_enabled is True
        assert boto_kwargs["aws_access_key_id"] == "alternator"

    def test_rejects_auth_with_legacy_boto_credentials(self) -> None:
        """Explicit auth cannot be mixed with raw boto credential kwargs."""
        boto_kwargs: dict[str, object] = {
            "aws_access_key_id": "alternator",
            "aws_secret_access_key": "secret",
        }

        with pytest.raises(ConfigurationError, match="Do not combine"):
            apply_auth(Auth.static_credentials("other", "secret"), boto_kwargs)
