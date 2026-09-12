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

"""Focused tests for durable CCM run ownership and recovery."""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import stat
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn, cast

import pytest

from tests.testinfra import run_state
from tests.testinfra.run_state import CcmRunState, CcmRunStateError, Manifest

if TYPE_CHECKING:
    from tests.testinfra.cluster_spec import ClusterSpec


@dataclass(frozen=True)
class _Topology:
    node_count: int = 3


@dataclass(frozen=True)
class _Spec:
    transports: frozenset[str] = frozenset({"HTTP", "HTTPS"})
    topology: _Topology = _Topology()


def _spec(*transports: str, node_count: int = 3) -> ClusterSpec:
    selected = frozenset(transports or ("HTTP", "HTTPS"))
    return cast(
        "ClusterSpec",
        _Spec(transports=selected, topology=_Topology(node_count=node_count)),
    )


def _crash_run(root: Path, instance_id: str = "crashed-cluster") -> tuple[Path, int]:
    program = textwrap.dedent(
        """
        import json
        import os
        import sys
        from pathlib import Path
        from types import SimpleNamespace

        from tests.testinfra.run_state import CcmRunState

        root = Path(sys.argv[1])
        instance_id = sys.argv[2]
        spec = SimpleNamespace(
            transports=frozenset({"HTTP", "HTTPS"}),
            topology=SimpleNamespace(node_count=3),
        )
        state = CcmRunState.open(root, lambda _run, _manifest: None)
        handle = state.begin_cluster(instance_id, spec)
        print(json.dumps({"run": str(state.run_directory), "id": handle.ccm_id}), flush=True)
        os._exit(0)
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", program, str(root), instance_id],
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    payload = json.loads(result.stdout)
    return Path(payload["run"]), int(payload["id"])


def _id_owner(root: Path, ccm_id: int) -> Path:
    return root / "ccm-id-locks" / f"{ccm_id}.owner"


def test_begin_complete_and_close_are_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "state"
    state = CcmRunState.open(root, lambda _run, _manifest: None)

    run_directory = state.run_directory
    owner = (run_directory / "OWNER").read_text(encoding="ascii")
    assert f"pid={os.getpid()}\n" in owner
    assert "start_ticks=" in owner
    assert "boot_id=" in owner
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE(run_directory.stat().st_mode) == 0o700

    handle = state.begin_cluster("owned-cluster", _spec())
    manifest = run_directory / "owned" / "owned-cluster.properties"
    assert 1 <= handle.ccm_id < 100
    assert manifest.is_file()
    assert _id_owner(root, handle.ccm_id).read_text(encoding="ascii") == (
        f"{run_directory.name}\n"
    )
    assert stat.S_IMODE(manifest.stat().st_mode) == 0o600

    state.complete_cluster(handle)
    state.complete_cluster(handle)
    assert not manifest.exists()
    assert not _id_owner(root, handle.ccm_id).exists()

    state.close()
    state.close()
    assert not run_directory.exists()


def test_active_runs_are_preserved_and_reserve_distinct_ids(tmp_path: Path) -> None:
    root = tmp_path / "state"
    recovered: list[Manifest] = []
    first = CcmRunState.open(root, lambda _run, manifest: recovered.append(manifest))
    first_handle = first.begin_cluster("first-cluster", _spec("HTTP"))

    second = CcmRunState.open(root, lambda _run, manifest: recovered.append(manifest))
    second_handle = second.begin_cluster("second-cluster", _spec("HTTP"))

    assert recovered == []
    assert first.run_directory.exists()
    assert 1 <= first_handle.ccm_id < 100
    assert 1 <= second_handle.ccm_id < 100
    assert second_handle.ccm_id != first_handle.ccm_id

    first.complete_cluster(first_handle)
    second.complete_cluster(second_handle)
    first.close()
    second.close()


def test_live_cross_process_runs_reserve_distinct_ids(tmp_path: Path) -> None:
    root = tmp_path / "state"
    program = textwrap.dedent(
        """
        import json
        import sys
        from pathlib import Path
        from types import SimpleNamespace

        from tests.testinfra.run_state import CcmRunState

        spec = SimpleNamespace(
            transports=frozenset({"HTTP"}),
            topology=SimpleNamespace(node_count=1),
        )
        state = CcmRunState.open(Path(sys.argv[1]), lambda _run, _manifest: None)
        handle = state.begin_cluster("child-cluster", spec)
        print(json.dumps({"run": str(state.run_directory), "id": handle.ccm_id}), flush=True)
        sys.stdin.read(1)
        state.complete_cluster(handle)
        state.close()
        """
    )
    child = subprocess.Popen(
        [sys.executable, "-c", program, str(root)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=False,
    )
    parent: CcmRunState | None = None
    parent_handle = None
    try:
        assert child.stdout is not None
        observation = child.stdout.readline()
        if not observation:
            _, errors = child.communicate(timeout=5)
            pytest.fail(f"child run-state process failed: {errors}")
        child_id = int(json.loads(observation)["id"])

        parent = CcmRunState.open(root, lambda _run, _manifest: None)
        parent_handle = parent.begin_cluster("parent-cluster", _spec("HTTP"))
        assert parent_handle.ccm_id != child_id
    finally:
        if parent is not None and parent_handle is not None:
            parent.complete_cluster(parent_handle)
            parent.close()
        if child.stdin is not None:
            child.stdin.write("x")
            child.stdin.close()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
        assert child.returncode == 0


def test_stale_run_is_recovered_and_its_id_is_reused(tmp_path: Path) -> None:
    root = tmp_path / "state"
    stale_run, stale_id = _crash_run(root)
    recovered: list[tuple[Path, Manifest]] = []

    state = CcmRunState.open(
        root, lambda run, manifest: recovered.append((run, manifest))
    )

    assert recovered == [(stale_run, Manifest("crashed-cluster", stale_id))]
    assert not stale_run.exists()
    assert not _id_owner(root, stale_id).exists()

    handle = state.begin_cluster("replacement-cluster", _spec())
    assert handle.ccm_id == stale_id
    state.complete_cluster(handle)
    state.close()


def test_zombie_owner_is_recovered_before_launcher_reaps_it(tmp_path: Path) -> None:
    root = tmp_path / "state"
    program = textwrap.dedent(
        """
        import json
        import os
        import sys
        from pathlib import Path
        from types import SimpleNamespace

        from tests.testinfra.run_state import CcmRunState

        spec = SimpleNamespace(
            transports=frozenset({"HTTP"}),
            topology=SimpleNamespace(node_count=1),
        )
        state = CcmRunState.open(Path(sys.argv[1]), lambda _run, _manifest: None)
        handle = state.begin_cluster("zombie-cluster", spec)
        print(json.dumps({"run": str(state.run_directory), "id": handle.ccm_id}), flush=True)
        os._exit(0)
        """
    )
    child = subprocess.Popen(
        [sys.executable, "-c", program, str(root)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=False,
    )
    replacement: CcmRunState | None = None
    try:
        assert child.stdout is not None
        observation = child.stdout.readline()
        if not observation:
            _, errors = child.communicate(timeout=5)
            pytest.fail(f"child run-state process failed: {errors}")
        payload = json.loads(observation)
        stale_run = Path(payload["run"])
        stale_id = int(payload["id"])

        deadline = time.monotonic() + 5
        while True:
            process_stat = (Path("/proc") / str(child.pid) / "stat").read_text(
                encoding="ascii"
            )
            command_end = process_stat.rfind(")")
            process_state = process_stat[command_end + 2 :].split()[0]
            if process_state == "Z":
                break
            if time.monotonic() >= deadline:
                pytest.fail("child owner process did not become a zombie")
            time.sleep(0.01)

        recovered: list[tuple[Path, Manifest]] = []
        replacement = CcmRunState.open(
            root, lambda run, manifest: recovered.append((run, manifest))
        )

        assert recovered == [(stale_run, Manifest("zombie-cluster", stale_id))]
        assert not stale_run.exists()
        assert not _id_owner(root, stale_id).exists()
    finally:
        if replacement is not None:
            replacement.close()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)


def test_exact_marked_same_uid_process_is_terminated_before_cleanup(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    stale_run, _ = _crash_run(root)
    environment = dict(os.environ)
    environment["SCYLLA_CCM_RUN_DIR"] = str(stale_run)
    sleeper = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"],
        env=environment,
        shell=False,
    )
    recovered: list[int] = []
    try:
        state = CcmRunState.open(
            root, lambda _run, manifest: recovered.append(manifest.ccm_id)
        )
        assert sleeper.wait(timeout=5) == -signal.SIGTERM
        assert recovered
        state.close()
    finally:
        if sleeper.poll() is None:
            sleeper.kill()
            sleeper.wait(timeout=5)


def test_failed_stale_cleanup_quarantines_run_and_reservation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "state"
    stale_run, stale_id = _crash_run(root)

    def fail_cleanup(_run: Path, _manifest: Manifest) -> None:
        raise RuntimeError("injected cleanup failure")

    state = CcmRunState.open(root, fail_cleanup)

    assert stale_run.exists()
    assert _id_owner(root, stale_id).read_text(encoding="ascii") == (
        f"{stale_run.name}\n"
    )
    assert "Preserving stale CCM run after cleanup failure" in capsys.readouterr().err

    handle = state.begin_cluster("unblocked-cluster", _spec())
    assert handle.ccm_id != stale_id
    state.complete_cluster(handle)
    state.close()


def test_malformed_owner_is_preserved_while_new_run_starts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "state"
    malformed = CcmRunState.open(root, lambda _run, _manifest: None)
    stale_run = malformed.run_directory
    (stale_run / "OWNER").write_text("not-owner-metadata\n", encoding="ascii")

    replacement = CcmRunState.open(root, lambda _run, _manifest: None)

    assert stale_run.exists()
    assert replacement.run_directory != stale_run
    assert "Preserving stale CCM run after cleanup failure" in capsys.readouterr().err
    replacement.close()


def test_malformed_reservation_is_skipped_during_id_allocation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    initial = CcmRunState.open(root, lambda _run, _manifest: None)
    initial.close()
    malformed_owner = _id_owner(root, 1)
    malformed_owner.write_text("not-a-run-name\n", encoding="ascii")
    malformed_owner.chmod(0o600)

    state = CcmRunState.open(root, lambda _run, _manifest: None)
    handle = state.begin_cluster("alternate-cluster", _spec())

    assert handle.ccm_id != 1
    assert malformed_owner.read_text(encoding="ascii") == "not-a-run-name\n"
    state.complete_cluster(handle)
    state.close()


def test_missing_run_reservation_is_released_only_when_address_is_free(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    stale_run, stale_id = _crash_run(root)
    shutil.rmtree(stale_run)
    assert _id_owner(root, stale_id).exists()

    state = CcmRunState.open(root, lambda _run, _manifest: None)

    assert not _id_owner(root, stale_id).exists()
    state.close()


def test_missing_run_reservation_stays_quarantined_while_address_is_busy(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "state"
    stale_run, stale_id = _crash_run(root)
    shutil.rmtree(stale_run)
    occupied = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    occupied.bind((f"127.0.{stale_id}.1", 8080))
    try:
        state = CcmRunState.open(root, lambda _run, _manifest: None)
        assert _id_owner(root, stale_id).exists()
        assert "without retired run" in capsys.readouterr().err
        handle = state.begin_cluster("alternate-cluster", _spec())
        assert handle.ccm_id != stale_id
        state.complete_cluster(handle)
        state.close()
    finally:
        occupied.close()

    replacement = CcmRunState.open(root, lambda _run, _manifest: None)
    assert not _id_owner(root, stale_id).exists()
    replacement.close()


def test_address_probe_checks_only_requested_alternator_transports() -> None:
    occupied: socket.socket | None = None
    selected_id: int | None = None
    for candidate_id in range(70, 99):
        candidate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            candidate.bind((f"127.0.{candidate_id}.1", 8080))
        except OSError:
            candidate.close()
            continue
        occupied = candidate
        selected_id = candidate_id
        break

    if occupied is None or selected_id is None:
        pytest.skip("No free loopback address available for bind-probe test")
    try:
        assert not CcmRunState.is_address_range_available(
            _spec("HTTP", node_count=1), selected_id, include_jmx=False
        )
        assert CcmRunState.is_address_range_available(
            _spec("HTTPS", node_count=1), selected_id, include_jmx=False
        )
    finally:
        occupied.close()


def test_default_root_honors_override_and_uses_private_permissions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    override = tmp_path / "override"
    monkeypatch.setenv("SCYLLA_CCM_ROOT", str(override))

    state = CcmRunState.open_default(lambda _run, _manifest: None)

    assert state.run_directory.parent == override / "runs"
    assert stat.S_IMODE(override.stat().st_mode) == 0o700
    state.close()


def test_broad_or_symbolic_roots_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(CcmRunStateError, match="Refusing unsafe CCM root"):
        CcmRunState.open(Path("/tmp"), lambda _run, _manifest: None)

    target = tmp_path / "real-root"
    target.mkdir()
    symbolic_root = tmp_path / "symbolic-root"
    symbolic_root.symlink_to(target, target_is_directory=True)
    with pytest.raises(CcmRunStateError, match="symbolic links"):
        CcmRunState.open(symbolic_root, lambda _run, _manifest: None)


def test_close_rejects_symlink_without_deleting_target(tmp_path: Path) -> None:
    root = tmp_path / "state"
    state = CcmRunState.open(root, lambda _run, _manifest: None)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "keep"
    sentinel.write_text("owned elsewhere", encoding="utf-8")
    unsafe = state.run_directory / "unsafe-link"
    unsafe.symlink_to(outside, target_is_directory=True)

    with pytest.raises(CcmRunStateError, match="symbolic link"):
        state.close()

    assert sentinel.read_text(encoding="utf-8") == "owned elsewhere"
    assert state.run_directory.exists()
    unsafe.unlink()
    state.close()


def test_one_manifest_per_run_and_safe_instance_names(tmp_path: Path) -> None:
    state = CcmRunState.open(tmp_path / "state", lambda _run, _manifest: None)
    handle = state.begin_cluster("first-cluster", _spec())

    with pytest.raises(CcmRunStateError, match="already owns"):
        state.begin_cluster("second-cluster", _spec())
    other = CcmRunState.open(tmp_path / "other-state", lambda _run, _manifest: None)
    with pytest.raises(ValueError, match="Unsafe CCM cluster instance ID"):
        other.begin_cluster("../escape", _spec())

    state.complete_cluster(handle)
    state.close()
    other.close()


def test_cancel_cluster_retires_only_matching_active_reservation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    state = CcmRunState.open(root, lambda _run, _manifest: None)

    state.cancel_cluster("not-yet-reserved")
    abandoned = state.begin_cluster("abandoned-cluster", _spec())
    manifest = state.run_directory / "owned" / "abandoned-cluster.properties"
    owner = _id_owner(root, abandoned.ccm_id)

    with pytest.raises(CcmRunStateError, match="this run owns abandoned-cluster"):
        state.cancel_cluster("different-cluster")
    assert manifest.is_file()
    assert owner.is_file()

    state.cancel_cluster("abandoned-cluster")
    state.cancel_cluster("abandoned-cluster")
    assert abandoned._completed  # noqa: SLF001 -- cancellation contract
    assert not manifest.exists()
    assert not owner.exists()

    replacement = state.begin_cluster("replacement-cluster", _spec())
    assert replacement.ccm_id == abandoned.ccm_id
    state.complete_cluster(replacement)
    state.close()


def test_keyboard_interrupt_during_manifest_publish_releases_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Interrupted manifest publication cannot leak reservation or file lock."""
    root = tmp_path / "state"
    state = CcmRunState.open(root, lambda _run, _manifest: None)
    original_write = run_state._write_exclusive  # noqa: SLF001 -- fault injection

    def interrupt_manifest(path: Path, contents: str) -> None:
        if path.suffix == ".properties":
            original_write(path, contents)
            raise KeyboardInterrupt
        original_write(path, contents)

    monkeypatch.setattr(run_state, "_write_exclusive", interrupt_manifest)
    with pytest.raises(KeyboardInterrupt):
        state.begin_cluster("interrupted", _spec())

    assert not list((root / "ccm-id-locks").glob("*.owner"))
    assert not list((state.run_directory / "owned").glob("*.properties"))
    monkeypatch.setattr(run_state, "_write_exclusive", original_write)
    handle = state.begin_cluster("replacement", _spec())
    state.complete_cluster(handle)
    state.close()


def test_interrupt_between_id_reservation_return_and_assignment_releases_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The state-owned pending lease closes the RETURN_VALUE handoff window."""
    root = tmp_path / "state"
    state = CcmRunState.open(root, lambda _run, _manifest: None)
    reserve_id = state._reserve_id  # noqa: SLF001 -- handoff fault injection

    def interrupt_after_reservation(spec: ClusterSpec, include_jmx: bool) -> NoReturn:
        reserve_id(spec, include_jmx)
        raise KeyboardInterrupt

    monkeypatch.setattr(state, "_reserve_id", interrupt_after_reservation)

    with pytest.raises(KeyboardInterrupt):
        state.begin_cluster("interrupted", _spec())

    assert not state._incomplete_id_leases  # noqa: SLF001 -- ownership invariant
    assert not list((root / "ccm-id-locks").glob("*.owner"))
    assert not list((state.run_directory / "owned").glob("*.properties"))

    monkeypatch.setattr(state, "_reserve_id", reserve_id)
    replacement = state.begin_cluster("replacement", _spec())
    state.complete_cluster(replacement)
    state.close()


def test_manifest_unlink_failure_preserves_id_and_blocks_reuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Uncertain manifest publication keeps its matching ID quarantined."""
    root = tmp_path / "state"
    state = CcmRunState.open(root, lambda _run, _manifest: None)
    original_write = run_state._write_exclusive  # noqa: SLF001 -- fault injection
    original_unlink = run_state._unlink_owned_file  # noqa: SLF001 -- fault injection

    def interrupt_manifest(path: Path, contents: str) -> None:
        original_write(path, contents)
        if path.suffix == ".properties":
            raise KeyboardInterrupt

    def fail_manifest_unlink(path: Path, *, missing_ok: bool = False) -> None:
        if path.suffix == ".properties":
            raise OSError("injected manifest unlink failure")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(run_state, "_write_exclusive", interrupt_manifest)
    monkeypatch.setattr(run_state, "_unlink_owned_file", fail_manifest_unlink)
    with pytest.raises(KeyboardInterrupt):
        state.begin_cluster("interrupted", _spec())

    owners = list((root / "ccm-id-locks").glob("*.owner"))
    manifests = list((state.run_directory / "owned").glob("*.properties"))
    assert len(owners) == 1
    assert len(manifests) == 1
    assert owners[0].read_text(encoding="ascii") == f"{state.run_directory.name}\n"

    with pytest.raises(CcmRunStateError, match="incomplete physical cluster ownership"):
        state.begin_cluster("replacement", _spec())
    assert list((root / "ccm-id-locks").glob("*.owner")) == owners
    assert list((state.run_directory / "owned").glob("*.properties")) == manifests

    competing_lock = run_state._try_file_lock(  # noqa: SLF001 -- lock assertion
        owners[0].with_suffix(".lock")
    )
    if competing_lock is not None:
        run_state._unlock_and_close(competing_lock)  # noqa: SLF001 -- test cleanup
    assert competing_lock is None

    monkeypatch.setattr(run_state, "_write_exclusive", original_write)
    monkeypatch.setattr(run_state, "_unlink_owned_file", original_unlink)
    original_unlink(manifests[0])
    retained_lease = state._incomplete_id_leases.pop()  # noqa: SLF001 -- test cleanup
    retained_lease.release(state.run_directory.name)
    state._incomplete_ownership = False  # noqa: SLF001 -- test cleanup
    state.close()


def test_keyboard_interrupt_after_id_owner_publish_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Interrupted owner publication removes exact owner before unlocking."""
    root = tmp_path / "state"
    state = CcmRunState.open(root, lambda _run, _manifest: None)
    original_write = run_state._write_exclusive  # noqa: SLF001 -- fault injection

    def interrupt_owner(path: Path, contents: str) -> None:
        original_write(path, contents)
        if path.suffix == ".owner":
            raise KeyboardInterrupt

    monkeypatch.setattr(run_state, "_write_exclusive", interrupt_owner)
    with pytest.raises(KeyboardInterrupt):
        state.begin_cluster("interrupted", _spec())

    assert not list((root / "ccm-id-locks").glob("*.owner"))
    monkeypatch.setattr(run_state, "_write_exclusive", original_write)
    handle = state.begin_cluster("replacement", _spec())
    state.complete_cluster(handle)
    state.close()
