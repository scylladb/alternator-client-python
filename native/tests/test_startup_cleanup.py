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

import copy
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
import hashlib
import importlib
import ctypes
import json
import os
import socket
import sys
import time
from pathlib import Path
import _alternator_rust as native

audit = ctypes.CDLL(None)
audit.startup_pair_inode.restype = ctypes.c_uint64
audit.startup_pair_device.restype = ctypes.c_uint64

def fds():
    result = {}
    for p in Path('/proc/self/fd').iterdir():
        try:
            s = p.stat()
            result[int(p.name)] = (s.st_dev, s.st_ino, s.st_mode)
        except FileNotFoundError:
            pass
    return result

def tasks():
    return {p.name for p in Path('/proc/self/task').iterdir()}

extension = importlib.import_module('_alternator_rust._alternator_rust') if hasattr(native, '__path__') else native
assert hashlib.sha256(Path(extension.__file__).read_bytes()).hexdigest() == sys.argv[3], 'wrong installed extension'
initial = fds()
threads = tasks()
assert not native._runtime_initialized()
messages = []
audit.startup_track_pairs(1)
try:
    native._runtime_id()
except Exception as error:
    assert type(error) is RuntimeError, type(error)
    assert "OS can't spawn worker thread" in str(error), str(error)
    messages.append(str(error))
else:
    raise AssertionError('first native construction did not fail')
mode = sys.argv[1]
expected_pairs = int(sys.argv[2])
if mode == 'extra_fd':
    leaked = os.open('/dev/null', os.O_RDONLY)
elif mode == 'second_pair':
    leaked = (ctypes.c_int * 2)()
    assert audit.socketpair(socket.AF_UNIX, socket.SOCK_STREAM | socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC, 0, leaked) == 0
elif mode == 'duplicate_global':
    assert audit.startup_pair_count() == 1, 'duplicate control requires actual global pair'
    leaked = os.dup(audit.startup_pair_fd(0))
audit.startup_track_pairs(0)
assert native._runtime_initialized()
assert expected_pairs in (0, 1)
assert audit.startup_pair_count() == expected_pairs, ('unexpected native socketpairs', audit.startup_pair_count(), expected_pairs)
assert audit.startup_invalid_pairs() == 0, 'unexpected native socketpair shape'
allowed = {audit.startup_pair_fd(i): (audit.startup_pair_device(i), audit.startup_pair_inode(i)) for i in range(2)} if expected_pairs else {}
assert len(allowed) == expected_pairs * 2
for _ in range(2):
    try:
        native._runtime_id()
    except Exception as error:
        assert type(error) is RuntimeError
        messages.append(str(error))
    else:
        raise AssertionError('cached failed runtime retried')
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
end = fds()
for fd, identity in initial.items():
    assert end.get(fd) == identity, ('baseline fd lost/replaced', fd, identity, end.get(fd))
extras = {fd: identity[:2] for fd, identity in end.items() if fd not in initial}
assert extras == allowed, ('unexplained fd identities', extras, allowed)
for fd, identity in allowed.items():
    assert end[fd][:2] == identity
# Match bounded kernel reclamation grace already present in Rust harness.
deadline = time.monotonic() + .1
while tasks() != threads and time.monotonic() < deadline:
    time.sleep(.001)
assert tasks() == threads, 'worker thread leak'
print(json.dumps({'module': native.__file__, 'mode': mode, 'cached_calls': len(messages), 'baseline': initial, 'allowed_pair': allowed, 'end': end, 'threads': sorted(tasks())}))
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


@pytest.fixture(scope="module")
def startup_context(tmp_path_factory: pytest.TempPathFactory) -> dict[str, object]:
    # No optional fallback or observation-derived allowance. Verify source,
    # actual normal-build feature proof, exact wheel and installed extension.
    context = Path(os.environ["ALTERNATOR_STARTUP_CONTEXT"])
    wheel = Path(os.environ["ALTERNATOR_STARTUP_WHEEL"])
    result = subprocess.run(
        [
            sys.executable,
            str(NATIVE / "tests/startup_context.py"),
            "verify",
            "--context",
            str(context),
            "--wheel",
            str(wheel),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    verified = json.loads(result.stdout)
    original = json.loads(context.read_text())
    controls = tmp_path_factory.mktemp("startup-context-controls")
    for mutation in (
        "count",
        "features",
        "upstream",
        "source",
        "wheel",
        "substitution",
    ):
        invalid = copy.deepcopy(original)
        if mutation == "count":
            invalid["expected_signal_pairs"] = 1 - invalid["expected_signal_pairs"]
        elif mutation == "features":
            # Coordinated context-only changes must not substitute for the
            # independently audited source contract and normal wheel features.
            invalid["tokio_features"] = [
                value
                for value in invalid["tokio_features"]
                if value not in ("signal", "process")
            ]
            if original["expected_signal_pairs"] == 0:
                invalid["tokio_features"].append("signal")
            invalid["tokio_fingerprint"]["features"] = json.dumps(
                invalid["tokio_features"]
            )
            invalid["expected_signal_pairs"] = 1 - original["expected_signal_pairs"]
        elif mutation == "upstream":
            invalid["upstream_source"] = {"sha256": "0" * 64}
        elif mutation == "source":
            invalid["source_files"]["src/runtime.rs"] = "0" * 64
        elif mutation == "substitution":
            invalid["upstream_source"] = (
                None
                if original["expected_signal_pairs"]
                else {
                    "sha256": "3f4d531e65aae12b7af576a8945d2551cde583653f4480cf319bc631e5993407"
                }
            )
            invalid["tokio_features"] = [
                value
                for value in invalid["tokio_features"]
                if value not in ("signal", "process")
            ]
            if original["expected_signal_pairs"] == 0:
                invalid["tokio_features"].append("signal")
            invalid["tokio_fingerprint"]["features"] = json.dumps(
                invalid["tokio_features"]
            )
            invalid["expected_signal_pairs"] = 1 - original["expected_signal_pairs"]
        else:
            invalid["wheel_sha256"] = "0" * 64
        path = controls / f"{mutation}.json"
        path.write_text(json.dumps(invalid))
        rejected = subprocess.run(
            [
                sys.executable,
                str(NATIVE / "tests/startup_context.py"),
                "verify",
                "--context",
                str(path),
                "--wheel",
                str(wheel),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert rejected.returncode == 1 and "startup context:" in rejected.stderr, (
            rejected.stdout + rejected.stderr
        )
    return verified


@pytest.mark.parametrize("failure", [1, 16, 32], ids=["first", "middle", "last"])
def test_installed_wheel_failed_startup_cleanup(
    thread_fault: Path, startup_context: dict[str, object], tmp_path: Path, failure: int
) -> None:
    expected = startup_context["expected_signal_pairs"]
    extension = startup_context["extension_sha256"]
    modes = ["normal", "extra_fd", "second_pair"]
    if expected == 1:
        modes.append("duplicate_global")
    for mode in modes:
        result = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                WHEEL_PROBE,
                mode,
                str(expected),
                str(extension),
            ],
            cwd=tmp_path,
            env=fault_env(thread_fault, failure),
            capture_output=True,
            text=True,
            timeout=20,
        )
        if mode == "normal":
            assert result.returncode == 0, result.stdout + result.stderr
        else:
            # A crashed/errored control is not evidence that the FD oracle caught
            # the leak: require its exact assertion and normal exit status 1.
            assert result.returncode == 1, result.stdout + result.stderr
            reason = (
                "unexpected native socketpairs"
                if mode == "second_pair"
                else "unexplained fd identities"
            )
            assert reason in result.stderr, result.stdout + result.stderr


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
