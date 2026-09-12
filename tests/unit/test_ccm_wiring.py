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

"""Static and hook contracts for two-phase CCM integration wiring."""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from tests import conftest
from tests.testinfra.ccm_install import CCM_COMMIT
from tests.testinfra.cluster_spec import ClusterSpec
from tests.testinfra.pool import TestClusters

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_make_runs_contracts_before_ordinary_suite_in_foreground() -> None:
    """First failing pytest command stops Make before ordinary test phase."""
    makefile = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")
    contract = "tests/integration/test_ccm_provisioning.py"
    ordinary = "--ignore=tests/integration/test_ccm_provisioning.py"

    assert "test-integration: ccm-install" in makefile
    assert makefile.index(contract) < makefile.index(ordinary)
    assert f"{contract} &" not in makefile
    assert f"{ordinary} &" not in makefile
    assert "--junitxml=test-results/ccm-provisioning.xml" in makefile
    assert "--junitxml=test-results/integration.xml" in makefile


def test_contract_phase_does_not_import_suite_cluster_config() -> None:
    """Private contracts cannot accidentally acquire ordinary suite lease."""
    source = (PROJECT_ROOT / "tests/integration/test_ccm_provisioning.py").read_text(
        encoding="utf-8"
    )
    assert "tests.integration.config" not in source


def test_ccm_commit_and_version_constants_do_not_drift() -> None:
    """Installer, Make, workflow cache, and typed defaults stay aligned."""
    makefile = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")
    workflow = (PROJECT_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    release_workflow = (PROJECT_ROOT / ".github/workflows/release.yml").read_text(
        encoding="utf-8"
    )
    make_commit = re.search(r"^SCYLLA_CCM_COMMIT := (\S+)$", makefile, re.MULTILINE)
    make_version = re.search(r"^SCYLLA_VERSION \?= (\S+)$", makefile, re.MULTILINE)
    assert make_commit is not None and make_commit.group(1) == CCM_COMMIT
    assert make_version is not None
    assert make_version.group(1) == ClusterSpec.DEFAULT_SCYLLA_VERSION
    assert CCM_COMMIT in workflow
    assert ClusterSpec.DEFAULT_SCYLLA_VERSION in workflow
    assert CCM_COMMIT in release_workflow
    assert ClusterSpec.DEFAULT_SCYLLA_VERSION in release_workflow


def test_linux_only_typechecking_is_separate_from_portable_code() -> None:
    """Windows-capable code is not checked under Linux platform assumptions."""
    makefile = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")

    assert "--exclude 'tests/unit/test_ccm_.*\\.py'" in makefile
    assert "uv run mypy --platform linux tests/testinfra/" in makefile


def test_release_validation_always_uploads_ccm_results() -> None:
    """Release failures retain the same CCM evidence as ordinary CI."""
    workflow = (PROJECT_ROOT / ".github/workflows/release.yml").read_text(
        encoding="utf-8"
    )

    assert "path: test-results/ccm/**" in workflow
    assert "path: test-results/*.xml" in workflow
    assert workflow.count("if: always()") >= 2


def test_session_finish_closes_pool_and_marks_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal pytest completion cannot report success after CCM cleanup failure."""
    calls = 0

    def fail_close() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("injected CCM cleanup failure")

    monkeypatch.setattr(TestClusters, "close_all", fail_close)
    plugin_manager = SimpleNamespace(get_plugin=lambda _name: None)
    session = SimpleNamespace(
        config=SimpleNamespace(pluginmanager=plugin_manager),
        exitstatus=pytest.ExitCode.OK,
    )

    conftest.pytest_sessionfinish(cast(Any, session), pytest.ExitCode.OK)

    assert calls == 1
    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED
