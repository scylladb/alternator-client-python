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

"""Linux-only child-local fault probes for installed wheels and Rust cleanup."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

NATIVE = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="pthread interposition and /proc probes require Linux",
)

WHEEL_PROBE = """
import asyncio
import json
from pathlib import Path
import _alternator_rust as native

fds = len(list(Path('/proc/self/fd').iterdir()))
threads = set(Path('/proc/self/task').iterdir())
assert not native._runtime_initialized()
messages = []
for _ in range(3):
    try:
        native._runtime_id()
    except Exception as error:
        assert type(error) is RuntimeError, type(error)
        assert "OS can't spawn worker thread" in str(error), str(error)
        messages.append(str(error))
    else:
        raise AssertionError('failed runtime startup retried or succeeded')
assert native._runtime_initialized()
async def check():
    try:
        await native._runtime_id_async()
    except Exception as error:
        assert type(error) is RuntimeError
        messages.append(str(error))
    else:
        raise AssertionError('cached async runtime failure succeeded')
asyncio.run(check())
assert len(set(messages)) == 1
assert len(list(Path('/proc/self/fd').iterdir())) == fds, 'driver FD leak'
assert set(Path('/proc/self/task').iterdir()) == threads, 'worker thread leak'
print(json.dumps({'cached_calls': len(messages), 'exception': 'RuntimeError', 'fds': fds}))
"""


@pytest.fixture(scope="module")
def thread_fault(tmp_path_factory: pytest.TempPathFactory) -> Path:
    compiler = shutil.which("cc")
    if compiler is None:
        pytest.skip("Linux startup fault injection requires a C compiler")
    library = tmp_path_factory.mktemp("thread-fault") / "fault.so"
    subprocess.run(
        [
            compiler,
            "-shared",
            "-fPIC",
            "-Wall",
            "-Wextra",
            "-Werror",
            str(NATIVE / "tests/fail_thread_creation.c"),
            "-ldl",
            "-o",
            str(library),
        ],
        check=True,
        timeout=30,
    )
    return library


def fault_env(library: Path, failure: int) -> dict[str, str]:
    # Only a disposable child sees the interposer or Tokio's worker-count override.
    return {
        **os.environ,
        "LD_PRELOAD": str(library),
        "FAIL_THREAD_ATTEMPT": str(failure),
        "TOKIO_WORKER_THREADS": "32",
    }


@pytest.mark.parametrize("failure", [1, 16, 32], ids=["first", "middle", "last"])
def test_installed_wheel_failed_startup_cleanup(
    thread_fault: Path, tmp_path: Path, failure: int
) -> None:
    subprocess.run(
        [sys.executable, "-I", "-c", WHEEL_PROBE],
        cwd=tmp_path,
        env=fault_env(thread_fault, failure),
        check=True,
        timeout=20,
    )


def test_core_cleanup_and_subsequent_runtime(
    thread_fault: Path, tmp_path: Path
) -> None:
    # This is an explicit native developer test; wheel-only checks above need no Rust.
    if shutil.which("cargo") is None:
        pytest.skip(
            "standalone Rust cleanup probe requires the native developer toolchain"
        )
    result = subprocess.run(
        [
            "cargo",
            "test",
            "--locked",
            "--test",
            "startup_cleanup",
            "--no-run",
            "--message-format=json",
        ],
        cwd=NATIVE,
        check=True,
        text=True,
        capture_output=True,
        timeout=120,
    )
    artifacts = [json.loads(line) for line in result.stdout.splitlines()]
    executable = next(
        item["executable"]
        for item in artifacts
        if item.get("reason") == "compiler-artifact" and item.get("executable")
    )
    for failure in (1, 16, 32):
        subprocess.run(
            [executable, "32"],
            cwd=tmp_path,
            env=fault_env(thread_fault, failure),
            check=True,
            timeout=20,
        )
