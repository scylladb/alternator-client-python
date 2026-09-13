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

"""Tests for pinned CCM installation and broken-cache repair."""

from __future__ import annotations

import contextlib
import fcntl
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tests.testinfra import ccm_install


def _configure_paths(monkeypatch: pytest.MonkeyPatch, root: Path) -> Path:
    venv = root / "scylla-ccm"
    executable = venv / "bin" / "ccm"
    monkeypatch.setattr(ccm_install, "CCM_VENV", venv)
    monkeypatch.setattr(ccm_install, "PINNED_CCM_PATH", executable)
    monkeypatch.setattr(ccm_install, "INSTALL_LOCK", root / "install.lock")
    monkeypatch.setattr(ccm_install, "INSTALL_MARKER", venv / ".install-complete")
    return executable


def _write_working_ccm(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        "raise SystemExit(0 if sys.argv[1:] == ['create', '--help'] else 1)\n",
        encoding="utf-8",
    )
    path.chmod(0o700)


def test_ccm_works_requires_launchable_create_help(tmp_path: Path) -> None:
    executable = tmp_path / "ccm"
    _write_working_ccm(executable)
    assert ccm_install.ccm_works(executable)

    executable.write_text("not executable\n", encoding="utf-8")
    executable.chmod(0o600)
    assert not ccm_install.ccm_works(executable)


def test_install_marker_must_be_regular_and_match_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = _configure_paths(monkeypatch, tmp_path)
    _write_working_ccm(executable)
    ccm_install.INSTALL_MARKER.write_text("wrong\n", encoding="ascii")
    assert not ccm_install.install_complete()

    ccm_install.INSTALL_MARKER.write_text(
        f"{ccm_install.CCM_COMMIT}\n", encoding="ascii"
    )
    assert ccm_install.install_complete()

    ccm_install.INSTALL_MARKER.unlink()
    target = tmp_path / "marker-target"
    target.write_text(f"{ccm_install.CCM_COMMIT}\n", encoding="ascii")
    ccm_install.INSTALL_MARKER.symlink_to(target)
    assert not ccm_install.install_complete()


def test_explicit_ccm_override_is_validated_without_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_paths(monkeypatch, tmp_path / "pinned")
    custom = tmp_path / "custom-ccm"
    _write_working_ccm(custom)
    monkeypatch.setenv("SCYLLA_CCM_PATH", str(custom))

    assert ccm_install.ensure_ccm() == custom


def test_relative_explicit_ccm_override_resolves_absolute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_paths(monkeypatch, tmp_path / "pinned")
    custom = tmp_path / "custom-ccm"
    _write_working_ccm(custom)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SCYLLA_CCM_PATH", "./custom-ccm")

    selected = ccm_install.ensure_ccm()

    assert selected == custom.resolve()
    assert selected.is_absolute()


def test_invalid_explicit_ccm_override_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_paths(monkeypatch, tmp_path / "pinned")
    missing = tmp_path / "missing-ccm"
    monkeypatch.setenv("SCYLLA_CCM_PATH", str(missing))

    with pytest.raises(RuntimeError, match="not a working CCM executable"):
        ccm_install.ensure_ccm()


def test_broken_pinned_environment_is_rebuilt_and_marked_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = _configure_paths(monkeypatch, tmp_path)
    monkeypatch.delenv("SCYLLA_CCM_PATH", raising=False)
    commands: list[list[str]] = []
    lock_descriptors: list[int] = []

    def fake_run(command: list[str], *, lock_descriptor: int | None = None) -> None:
        commands.append(command)
        assert lock_descriptor is not None
        os.fstat(lock_descriptor)
        lock_descriptors.append(lock_descriptor)
        if command[1] == "venv":
            (ccm_install.CCM_VENV / "bin").mkdir(parents=True)
        else:
            _write_working_ccm(executable)

    monkeypatch.setattr(ccm_install, "_run_checked", fake_run)

    assert ccm_install.ensure_ccm() == executable
    assert [command[1] for command in commands] == ["venv", "pip"]
    assert len(lock_descriptors) == 2
    assert lock_descriptors[0] == lock_descriptors[1]
    assert ccm_install.INSTALL_MARKER.read_text(encoding="ascii") == (
        f"{ccm_install.CCM_COMMIT}\n"
    )
    assert os.stat(ccm_install.INSTALL_MARKER).st_mode & 0o777 == 0o600

    commands.clear()
    assert ccm_install.ensure_ccm() == executable
    assert commands == []


def test_concurrent_installers_share_one_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = _configure_paths(monkeypatch, tmp_path)
    monkeypatch.delenv("SCYLLA_CCM_PATH", raising=False)
    commands: list[list[str]] = []
    command_lock = threading.Lock()

    def fake_run(command: list[str], *, lock_descriptor: int | None = None) -> None:
        assert lock_descriptor is not None
        with command_lock:
            commands.append(command)
        if command[1] == "venv":
            (ccm_install.CCM_VENV / "bin").mkdir(parents=True)
        else:
            _write_working_ccm(executable)

    monkeypatch.setattr(ccm_install, "_run_checked", fake_run)

    with ThreadPoolExecutor(max_workers=2) as executor:
        selected = list(executor.map(lambda _index: ccm_install.ensure_ccm(), range(2)))

    assert selected == [executable, executable]
    assert [command[1] for command in commands] == ["venv", "pip"]


def test_install_lock_symlink_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_paths(monkeypatch, tmp_path)
    monkeypatch.delenv("SCYLLA_CCM_PATH", raising=False)
    target = tmp_path / "foreign-lock"
    target.write_text("do not touch", encoding="utf-8")
    ccm_install.INSTALL_LOCK.symlink_to(target)

    with pytest.raises(OSError):
        ccm_install.ensure_ccm()

    assert target.read_text(encoding="utf-8") == "do not touch"


def test_stalled_installer_is_terminated_and_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Network-backed CCM installation cannot block setup indefinitely."""
    signals: list[int] = []

    class StalledProcess:
        pid = 123_456_789
        waits = 0

        def wait(self, timeout: float) -> int:
            self.waits += 1
            if self.waits == 1:
                raise subprocess.TimeoutExpired(["uv"], timeout)
            return -signal.SIGTERM

    process = StalledProcess()
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(
        os,
        "killpg",
        lambda _process_group, signal_number: signals.append(signal_number),
    )

    with pytest.raises(RuntimeError, match="Timed out installing scylla-ccm"):
        ccm_install._run_checked(["uv", "pip", "install", "ccm"])  # noqa: SLF001 -- timeout contract

    assert signals == [signal.SIGTERM, signal.SIGKILL]
    assert process.waits == 2


def test_installer_child_retains_lock_after_parent_is_killed(tmp_path: Path) -> None:
    """A detached installer keeps successors out if its Python parent dies."""
    lock_path = tmp_path / "install.lock"
    ready_path = tmp_path / "installer.pid"
    child_program = textwrap.dedent(
        """
        import os
        import sys
        import time
        from pathlib import Path

        Path(sys.argv[1]).write_text(str(os.getpid()), encoding="ascii")
        time.sleep(30)
        """
    )
    parent_program = textwrap.dedent(
        """
        import fcntl
        import os
        import sys
        from pathlib import Path

        from tests.testinfra import ccm_install

        lock_path = Path(sys.argv[1])
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        ccm_install._run_checked(
            [sys.executable, "-c", sys.argv[3], sys.argv[2]],
            lock_descriptor=descriptor,
        )
        """
    )
    parent = subprocess.Popen(
        [
            sys.executable,
            "-c",
            parent_program,
            str(lock_path),
            str(ready_path),
            child_program,
        ],
        cwd=ccm_install.PROJECT_ROOT,
        stderr=subprocess.PIPE,
        text=True,
    )
    installer_pid: int | None = None
    probe_descriptor: int | None = None
    try:
        deadline = time.monotonic() + 5
        while not ready_path.exists():
            if parent.poll() is not None:
                assert parent.stderr is not None
                pytest.fail(f"installer parent failed: {parent.stderr.read()}")
            if time.monotonic() >= deadline:
                pytest.fail("installer child did not start")
            time.sleep(0.01)
        installer_pid = int(ready_path.read_text(encoding="ascii"))

        os.kill(parent.pid, signal.SIGKILL)
        assert parent.wait(timeout=5) == -signal.SIGKILL

        probe_descriptor = os.open(lock_path, os.O_RDWR)
        with pytest.raises(BlockingIOError):
            fcntl.flock(probe_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)

        os.killpg(installer_pid, signal.SIGTERM)
        deadline = time.monotonic() + 5
        while True:
            try:
                fcntl.flock(probe_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    pytest.fail("installer child did not release the install lock")
                time.sleep(0.01)
            else:
                break
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait(timeout=5)
        if installer_pid is not None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(installer_pid, signal.SIGKILL)
        if probe_descriptor is not None:
            os.close(probe_descriptor)
