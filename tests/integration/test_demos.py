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

"""Run repository examples against CCM-provisioned integration endpoint."""

from __future__ import annotations

import asyncio

import pytest

from examples import async_demo, capability_configuration, sync_demo
from tests.integration.config import (
    SCYLLA_HOST,
    SCYLLA_PORT,
    SKIP_INTEGRATION,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


def test_sync_demo(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run synchronous example against native test cluster."""
    monkeypatch.setenv("SCYLLA_HOST", SCYLLA_HOST)
    monkeypatch.setenv("SCYLLA_PORT", str(SCYLLA_PORT))
    sync_demo.main()


def test_async_demo(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run asynchronous example against native test cluster."""
    monkeypatch.setenv("SCYLLA_HOST", SCYLLA_HOST)
    monkeypatch.setenv("SCYLLA_PORT", str(SCYLLA_PORT))
    asyncio.run(async_demo.main())


def test_capability_configuration_demo() -> None:
    """Build every offline capability configuration example."""
    capability_configuration.main()
