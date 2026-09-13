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

"""Platform-neutral tests for shared pytest hooks."""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest

from tests import conftest

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_session_finish_does_not_import_ccm_pool_on_non_linux(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unrelated test sessions must not import Linux-only CCM infrastructure."""
    fake_testinfra = ModuleType("tests.testinfra")
    fake_pool = ModuleType("tests.testinfra.pool")
    monkeypatch.setitem(sys.modules, "tests.testinfra", fake_testinfra)
    monkeypatch.setitem(sys.modules, "tests.testinfra.pool", fake_pool)
    monkeypatch.setattr(sys, "platform", "win32")
    session = SimpleNamespace(exitstatus=pytest.ExitCode.OK)

    conftest.pytest_sessionfinish(cast(Any, session), pytest.ExitCode.OK)

    assert session.exitstatus == pytest.ExitCode.OK


def test_non_linux_collection_excludes_ccm_before_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Linux-only modules are ignored while portable unit modules remain visible."""
    monkeypatch.setattr(sys, "platform", "win32")
    config = cast(pytest.Config, SimpleNamespace())

    assert conftest.pytest_ignore_collect(
        PROJECT_ROOT / "tests/unit/test_ccm_run_state.py", config
    )
    assert conftest.pytest_ignore_collect(PROJECT_ROOT / "tests/integration", config)
    assert (
        conftest.pytest_ignore_collect(
            PROJECT_ROOT / "tests/unit/test_config.py", config
        )
        is None
    )


def test_linux_collection_keeps_ccm_suites(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Supported hosts continue collecting infrastructure and integration tests."""
    monkeypatch.setattr(sys, "platform", "linux")

    assert (
        conftest.pytest_ignore_collect(
            PROJECT_ROOT / "tests/unit/test_ccm_run_state.py",
            cast(pytest.Config, SimpleNamespace()),
        )
        is None
    )
