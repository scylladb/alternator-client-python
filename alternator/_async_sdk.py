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

"""Private client-only adapter for the async AWS SDK session boundary.

This preserves the ``Session().client(...)`` shape used with aioboto3 while
delegating to a modern aiobotocore release. It is intentionally not a drop-in
aioboto3 replacement and does not provide resources or service customizations.

TODO(#115): Remove this adapter when aioboto3 supports the required Botocore
release family.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from aiobotocore.session import AioSession, ClientCreatorContext
    from types_aiobotocore_dynamodb import DynamoDBClient as AsyncDynamoDBClient


class _SessionAdapter:
    """Adapt aiobotocore to the client-only session API used by this package."""

    def __init__(self) -> None:
        try:
            from aiobotocore.session import get_session
        except ImportError as e:
            raise ImportError(
                "aiobotocore is required for async support. "
                "Install with: pip install alternator-client[async]"
            ) from e

        self._session: AioSession = get_session()

    def client(
        self,
        service_name: Literal["dynamodb"],
        **kwargs: Any,  # noqa: ANN401 -- AWS SDK client kwargs are open-ended
    ) -> ClientCreatorContext[AsyncDynamoDBClient]:
        """Return the underlying aiobotocore client context unchanged."""
        return self._session.create_client(service_name, **kwargs)
