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

"""Run explicitly after installing a built wheel; never skip a missing extension."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import _alternator_rust as native
import pytest


def _run_isolated(code: str, tmp_path: Path) -> None:
    # Bound the whole process, including native waits and executor/loop cleanup.
    subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            code,
        ],
        cwd=tmp_path,
        check=True,
        timeout=20,
    )


def test_wheel_import_and_lazy_runtime(tmp_path: Path) -> None:
    _run_isolated(
        """
import importlib.metadata
import _alternator_rust as native
assert native.INTERFACE_VERSION == 1
assert native.__version__ == importlib.metadata.version('alternator-client-rust-backend')
assert not native._runtime_initialized()
assert '_alternator_rust' in native.__name__
""",
        tmp_path,
    )


def test_sync_callers_share_runtime(tmp_path: Path) -> None:
    _run_isolated(
        """
from concurrent.futures import ThreadPoolExecutor
import _alternator_rust as native

with ThreadPoolExecutor(max_workers=8) as workers:
    ids = list(workers.map(lambda _: native._runtime_id(), range(32)))
assert len(set(ids)) == 1
assert native._runtime_initialized()
""",
        tmp_path,
    )


def test_async_and_sync_share_runtime_across_loops(tmp_path: Path) -> None:
    _run_isolated(
        """
import asyncio
import _alternator_rust as native

async def run():
    expected = native._runtime_id()
    ids = await asyncio.gather(*(native._runtime_id_async() for _ in range(16)))
    assert all(identity == expected for identity in ids)
    return expected

assert asyncio.run(run()) == asyncio.run(run())
""",
        tmp_path,
    )


def test_async_bridge_requires_a_running_loop() -> None:
    with pytest.raises(RuntimeError, match="no running event loop"):
        native._runtime_id_async()
