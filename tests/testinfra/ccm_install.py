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

"""Install and validate repository-pinned scylla-ccm without a shell script."""

from __future__ import annotations

import contextlib
import fcntl
import os
import shutil
import signal
import stat
import subprocess
import tempfile
from pathlib import Path

CCM_COMMIT = "d15a2fab9d22fffad8a30c806a7c8e1632e58aae"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CCM_VENV = PROJECT_ROOT / "bin" / f"scylla-ccm-{CCM_COMMIT}"
PINNED_CCM_PATH = CCM_VENV / "bin" / "ccm"
INSTALL_LOCK = CCM_VENV.with_name(f"{CCM_VENV.name}.install.lock")
INSTALL_MARKER = CCM_VENV / ".install-complete"
INSTALL_TIMEOUT = 600.0
INSTALL_TERMINATION_TIMEOUT = 5.0


def _resolve_executable(value: str | Path) -> Path | None:
    candidate = str(value)
    resolved = shutil.which(candidate) if os.sep not in candidate else candidate
    if resolved is None:
        return None
    path = Path(resolved)
    try:
        mode = path.stat().st_mode
    except OSError:
        return None
    if not stat.S_ISREG(mode) or not os.access(path, os.X_OK):
        return None
    try:
        return path.resolve(strict=True)
    except OSError:
        return None


def ccm_works(value: str | Path) -> bool:
    """Return whether an executable launches CCM's create help command."""
    executable = _resolve_executable(value)
    if executable is None:
        return False
    try:
        result = subprocess.run(
            [str(executable), "create", "--help"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def install_complete() -> bool:
    """Validate pinned marker and executable, including broken-cache repair."""
    try:
        marker_stat = INSTALL_MARKER.lstat()
        if not stat.S_ISREG(marker_stat.st_mode) or INSTALL_MARKER.is_symlink():
            return False
        if INSTALL_MARKER.read_text(encoding="ascii").strip() != CCM_COMMIT:
            return False
    except OSError:
        return False
    return ccm_works(PINNED_CCM_PATH)


def _run_checked(command: list[str], *, lock_descriptor: int | None = None) -> None:
    """Run one installer while letting it retain the install lock on parent death."""
    try:
        pass_fds = () if lock_descriptor is None else (lock_descriptor,)
        process = subprocess.Popen(
            command,
            start_new_session=True,
            pass_fds=pass_fds,
        )
    except FileNotFoundError as error:
        raise RuntimeError(
            "uv is required to install scylla-ccm: https://docs.astral.sh/uv/"
        ) from error
    try:
        return_code = process.wait(timeout=INSTALL_TIMEOUT)
    except subprocess.TimeoutExpired as error:
        _terminate_process_group(process)
        raise RuntimeError(
            f"Timed out installing scylla-ccm after {INSTALL_TIMEOUT:g} seconds"
        ) from error
    except BaseException:
        try:
            _terminate_process_group(process)
        except BaseException as cleanup_error:
            raise RuntimeError(
                "Interrupted CCM installation process could not be reaped"
            ) from cleanup_error
        raise
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    """Terminate installer group and reap direct child."""
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=INSTALL_TERMINATION_TIMEOUT)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        try:
            process.wait(timeout=INSTALL_TERMINATION_TIMEOUT)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(
                "CCM installation process group did not terminate"
            ) from error
        return
    # Direct installer may exit after TERM while a descendant ignores it.
    # This process group belongs to the fresh installer session.
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)


def _write_marker() -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".install-complete.", dir=CCM_VENV
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="ascii") as stream:
            stream.write(f"{CCM_COMMIT}\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, INSTALL_MARKER)
    finally:
        temporary.unlink(missing_ok=True)


def ensure_ccm() -> Path:
    """Return working configured CCM, installing pinned revision when needed."""
    if shutil.which("openssl") is None:
        raise RuntimeError("openssl is required for CCM integration tests")
    configured = os.environ.get("SCYLLA_CCM_PATH")
    if configured and Path(configured) != PINNED_CCM_PATH:
        executable = _resolve_executable(configured)
        if executable is None or not ccm_works(executable):
            raise RuntimeError(
                f"SCYLLA_CCM_PATH is not a working CCM executable: {configured}"
            )
        return executable

    INSTALL_LOCK.parent.mkdir(parents=True, exist_ok=True)
    parent_stat = INSTALL_LOCK.parent.lstat()
    if (
        not stat.S_ISDIR(parent_stat.st_mode)
        or INSTALL_LOCK.parent.is_symlink()
        or parent_stat.st_uid not in {os.getuid(), os.geteuid()}
    ):
        raise RuntimeError(f"Unsafe CCM install directory: {INSTALL_LOCK.parent}")
    if CCM_VENV.is_symlink():
        raise RuntimeError(f"Unsafe CCM virtual environment: {CCM_VENV}")
    flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    lock_descriptor = os.open(INSTALL_LOCK, flags, 0o600)
    try:
        lock_stat = os.fstat(lock_descriptor)
        if not stat.S_ISREG(lock_stat.st_mode) or lock_stat.st_uid not in {
            os.getuid(),
            os.geteuid(),
        }:
            raise RuntimeError(f"Unsafe CCM install lock: {INSTALL_LOCK}")
        os.fchmod(lock_descriptor, 0o600)
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
        if not install_complete():
            _run_checked(
                ["uv", "venv", "--clear", str(CCM_VENV)],
                lock_descriptor=lock_descriptor,
            )
            _run_checked(
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    str(CCM_VENV / "bin" / "python"),
                    f"git+https://github.com/scylladb/scylla-ccm.git@{CCM_COMMIT}",
                ],
                lock_descriptor=lock_descriptor,
            )
            if not ccm_works(PINNED_CCM_PATH):
                raise RuntimeError("Installed CCM entry point failed its launch check")
            _write_marker()
        return PINNED_CCM_PATH
    finally:
        os.close(lock_descriptor)


def main() -> None:
    """Install CCM and print selected executable."""
    print(f"Using CCM executable: {ensure_ccm()}")


if __name__ == "__main__":
    main()
