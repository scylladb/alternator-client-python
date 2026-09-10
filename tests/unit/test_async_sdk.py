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

"""Contract tests for the temporary private async SDK adapter."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import alternator
from alternator._async_sdk import _SessionAdapter


def test_client_forwards_service_and_kwargs_without_wrapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The adapter preserves the aioboto3 client-session boundary."""
    from aiobotocore import session as aiobotocore_session

    client_context = MagicMock()
    client_context.__aenter__ = AsyncMock()
    client_context.__aexit__ = AsyncMock()
    sdk_session = MagicMock()
    sdk_session.create_client.return_value = client_context
    monkeypatch.setattr(aiobotocore_session, "get_session", lambda: sdk_session)

    config = MagicMock()
    result = _SessionAdapter().client(
        "dynamodb",
        endpoint_url="http://127.0.0.1:8000",
        region_name="us-east-1",
        config=config,
        aws_access_key_id="access-key",
        aws_secret_access_key="secret-key",
    )

    assert result is client_context
    client_context.__aenter__.assert_not_awaited()
    client_context.__aexit__.assert_not_awaited()
    sdk_session.create_client.assert_called_once_with(
        "dynamodb",
        endpoint_url="http://127.0.0.1:8000",
        region_name="us-east-1",
        config=config,
        aws_access_key_id="access-key",
        aws_secret_access_key="secret-key",
    )


def test_missing_aiobotocore_error_names_async_extra() -> None:
    """The missing-dependency hint names the installable project extra."""
    with (
        patch.dict("sys.modules", {"aiobotocore.session": None}),
        pytest.raises(
            ImportError,
            match=r"pip install alternator-client\[async\]",
        ),
    ):
        _SessionAdapter()


def test_adapter_is_not_part_of_the_public_package_api() -> None:
    """The temporary adapter must remain an internal implementation detail."""
    assert "_SessionAdapter" not in alternator.__all__
    assert not hasattr(alternator, "_SessionAdapter")
    assert not hasattr(_SessionAdapter, "resource")
