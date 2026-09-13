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

"""Durable ownership and crash recovery for CCM-backed test clusters.

State stored here is deliberately independent from CCM's own metadata.  It lets a
later test process prove that a run is stale, reap only processes carrying that
run's exact marker, and release its loopback-address reservation only after CCM
cleanup has succeeded.

This module is Linux-only.  All filesystem mutation is constrained to private,
current-user-owned directories which do not traverse symbolic links.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import os
import re
import signal
import socket
import stat
import sys
import tempfile
import threading
import time
import traceback
import uuid
from collections.abc import Callable, Iterator
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from tests.testinfra.cluster_spec import ClusterSpec


_FORMAT_VERSION: Final = 1
_MAXIMUM_METADATA_BYTES: Final = 4096
_RUN_NAME_RE: Final = re.compile(r"ccm-runtime\.[0-9a-f]{32}\Z")
_INSTANCE_ID_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,127}\Z")
_BOOT_ID_RE: Final = re.compile(r"[0-9a-fA-F-]{36}\Z")
_STAGING_RE: Final = re.compile(
    r"\.owner-[A-Za-z0-9][A-Za-z0-9._-]{0,200}-[A-Za-z0-9_]{6,}\.tmp\Z"
)
_SCAN_MUTEX: Final = threading.RLock()

_TERM_GRACE_SECONDS: Final = 2.0
_KILL_GRACE_SECONDS: Final = 5.0
_PROCESS_POLL_SECONDS: Final = 0.05

_STORAGE_PORT: Final = 7000
_JMX_PORT: Final = 7199
_CQL_PORT: Final = 9042
_PROMETHEUS_PORT: Final = 9180
_API_PORT: Final = 10000
_SHARD_AWARE_CQL_PORT: Final = 19042
_ALTERNATOR_HTTP_PORT: Final = 8080
_ALTERNATOR_HTTPS_PORT: Final = 8043
_MAXIMUM_NODE_COUNT: Final = 9


class CcmRunStateError(RuntimeError):
    """CCM ownership state could not be changed without risking foreign data."""


@dataclass(frozen=True)
class Manifest:
    """Durable identity needed to clean one stale physical cluster."""

    instance_id: str
    ccm_id: int


StaleClusterCleanup = Callable[[Path, Manifest], None]


@dataclass(frozen=True)
class _Owner:
    pid: int
    start_ticks: int
    boot_id: str


@dataclass(frozen=True)
class _MarkedProcess:
    pid: int
    start_ticks: int


@dataclass
class _IdLease:
    ccm_id: int
    owner_path: Path
    lock_fd: int
    released: bool = False

    def release(self, expected_owner: str) -> None:
        """Delete durable owner before releasing in-memory file lock."""
        if self.released:
            return
        actual_owner = _read_reservation_owner(self.owner_path)
        if actual_owner is None and self.lock_fd >= 0:
            _unlock_and_close(self.lock_fd)
            self.lock_fd = -1
            self.released = True
            return
        if actual_owner != expected_owner:
            raise CcmRunStateError(
                f"CCM ID {self.ccm_id} reservation changed owner at {self.owner_path}"
            )
        _unlink_owned_file(self.owner_path)
        _fsync_directory(self.owner_path.parent)
        _unlock_and_close(self.lock_fd)
        self.lock_fd = -1
        self.released = True


@dataclass
class ClusterHandle:
    """Opaque ownership token returned by :meth:`CcmRunState.begin_cluster`."""

    instance_id: str
    ccm_id: int
    _owner: CcmRunState = field(repr=False)
    _manifest_path: Path = field(repr=False)
    _id_lease: _IdLease = field(repr=False)
    _completed: bool = field(default=False, repr=False)


class CcmRunState:
    """One process run, one physical-cluster manifest, and one CCM ID lease."""

    def __init__(
        self,
        runs_directory: Path,
        id_locks_directory: Path,
        scan_lock_path: Path,
        run_directory: Path,
    ) -> None:
        self._runs_directory = runs_directory
        self._id_locks_directory = id_locks_directory
        self._scan_lock_path = scan_lock_path
        self._run_directory = run_directory
        self._run_name = run_directory.name
        self._manifests_directory = run_directory / "owned"
        self._clusters: dict[int, ClusterHandle] = {}
        self._incomplete_id_leases: list[_IdLease] = []
        self._incomplete_ownership = False
        self._closed = False
        self._mutex = threading.RLock()

    @classmethod
    def open_default(cls, cleanup: StaleClusterCleanup) -> CcmRunState:
        """Open default per-user state root, honoring ``SCYLLA_CCM_ROOT``."""
        configured = os.environ.get("SCYLLA_CCM_ROOT", "").strip()
        requested = (
            Path(configured)
            if configured
            else Path("/tmp") / f"alternator-client-python-ccm-{os.getuid()}"
        )
        return cls.open(requested, cleanup)

    @classmethod
    def open(
        cls,
        root: str | os.PathLike[str],
        cleanup: StaleClusterCleanup,
    ) -> CcmRunState:
        """Recover stale runs under ``root``, then publish this process's run."""
        _require_linux()
        if not callable(cleanup):
            raise TypeError("cleanup must be callable")

        state_root = _prepare_root(Path(root))
        runs_directory = _prepare_child_directory(state_root, "runs")
        id_locks_directory = _prepare_child_directory(state_root, "ccm-id-locks")
        scan_lock_path = state_root / "scan.lock"

        with _file_lock(scan_lock_path):
            cls._recover_stale_runs(runs_directory, id_locks_directory, cleanup)
            cls._release_reservations_for_missing_runs(
                runs_directory, id_locks_directory
            )
            run_directory = cls._create_run_directory(runs_directory)

        return cls(
            runs_directory,
            id_locks_directory,
            scan_lock_path,
            run_directory,
        )

    @property
    def run_directory(self) -> Path:
        """Private directory inherited by every process belonging to this run."""
        return self._run_directory

    def begin_cluster(
        self,
        instance_id: str,
        spec: ClusterSpec,
        include_jmx: bool = True,
    ) -> ClusterHandle:
        """Atomically reserve one CCM ID and publish one cluster manifest."""
        _validate_instance_id(instance_id)
        with self._mutex:
            self._ensure_open()
            if self._clusters:
                raise CcmRunStateError("This CCM run already owns a physical cluster")
            if self._incomplete_ownership:
                raise CcmRunStateError(
                    "This CCM run has incomplete physical cluster ownership"
                )

            manifest_path = self._manifests_directory / f"{instance_id}.properties"
            id_lease: _IdLease | None = None
            handle: ClusterHandle | None = None
            try:
                id_lease = self._reserve_id(spec, include_jmx)
                _write_exclusive(
                    manifest_path,
                    "format="
                    f"{_FORMAT_VERSION}\n"
                    f"instance_id={instance_id}\n"
                    f"ccm_id={id_lease.ccm_id}\n",
                )
                handle = ClusterHandle(
                    instance_id=instance_id,
                    ccm_id=id_lease.ccm_id,
                    _owner=self,
                    _manifest_path=manifest_path,
                    _id_lease=id_lease,
                )
                self._clusters[id_lease.ccm_id] = handle
                self._forget_incomplete_id_lease(id_lease)
                return handle
            except BaseException:
                # _reserve_id publishes the lease into state before returning,
                # so even an exception between RETURN_VALUE and STORE_FAST can
                # be rolled back without losing its descriptor or owner file.
                if id_lease is None and len(self._incomplete_id_leases) == 1:
                    id_lease = self._incomplete_id_leases[0]
                if id_lease is None:
                    raise
                self._clusters.pop(id_lease.ccm_id, None)
                manifest_rollback_failed = False
                try:
                    _unlink_owned_file(manifest_path, missing_ok=True)
                except BaseException:
                    # The manifest may have been published even when its writer
                    # raised.  Keep the matching reservation and lock alive so a
                    # later stale-run recovery can retire that exact ownership.
                    self._retain_incomplete_id_lease(id_lease)
                    manifest_rollback_failed = True
                if manifest_rollback_failed:
                    raise
                try:
                    id_lease.release(self._run_name)
                except BaseException:
                    self._retain_incomplete_id_lease(id_lease)
                else:
                    self._forget_incomplete_id_lease(id_lease)
                raise

    def complete_cluster(self, handle: ClusterHandle) -> None:
        """Retire cluster config, manifest, and ID reservation in safe order."""
        with self._mutex:
            if handle._owner is not self:
                raise ValueError("Cluster handle is not owned by this CCM run")
            if handle._completed:
                return
            if self._clusters.get(handle.ccm_id) is not handle:
                raise ValueError("Cluster handle is not active in this CCM run")

            self._retire_cluster_config(self._run_directory, handle.instance_id)
            _unlink_owned_file(handle._manifest_path, missing_ok=True)
            handle._id_lease.release(self._run_name)
            self._clusters.pop(handle.ccm_id, None)
            handle._completed = True

    def cancel_cluster(self, instance_id: str) -> None:
        """Cancel this run's matching reservation before provisioning starts."""
        _validate_instance_id(instance_id)
        with self._mutex:
            self._ensure_open()
            if not self._clusters:
                return

            handle = next(iter(self._clusters.values()))
            if handle.instance_id != instance_id:
                raise CcmRunStateError(
                    "Cannot cancel CCM ownership for "
                    f"{instance_id}: this run owns {handle.instance_id}"
                )
            self.complete_cluster(handle)

    def close(self) -> None:
        """Remove empty run state. Repeated close is harmless."""
        with self._mutex:
            if self._closed:
                return
            if self._clusters:
                raise CcmRunStateError(
                    "Cannot retire a CCM run while it still owns a cluster"
                )
            if self._incomplete_ownership:
                raise CcmRunStateError(
                    "Cannot retire a CCM run with incomplete ownership publication"
                )

            with _file_lock(self._scan_lock_path):
                try:
                    _validate_child_directory(
                        self._runs_directory, self._run_directory, "run directory"
                    )
                except FileNotFoundError:
                    pass
                else:
                    _delete_owned_tree(self._run_directory)
            self._closed = True

    def __enter__(self) -> CcmRunState:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback_object: object | None,
    ) -> None:
        self.close()

    @classmethod
    def is_address_range_available(
        cls,
        spec: ClusterSpec,
        ccm_id: int,
        include_jmx: bool = True,
    ) -> bool:
        """Prove every port in one nine-node loopback range can be bound."""
        if isinstance(ccm_id, bool) or not 1 <= ccm_id < 100:
            return False

        transports = _transport_names(spec)
        topology = getattr(spec, "topology", None)
        node_count = getattr(topology, "node_count", None)
        if callable(node_count):
            node_count = node_count()
        if node_count is not None and (
            isinstance(node_count, bool)
            or not isinstance(node_count, int)
            or not 1 <= node_count <= _MAXIMUM_NODE_COUNT
        ):
            return False

        return cls._address_range_available_for_flags(
            ccm_id,
            http="HTTP" in transports,
            https="HTTPS" in transports,
            include_jmx=include_jmx,
        )

    @staticmethod
    def _address_range_available_for_flags(
        ccm_id: int,
        *,
        http: bool,
        https: bool,
        include_jmx: bool,
    ) -> bool:
        if isinstance(ccm_id, bool) or not 1 <= ccm_id < 100:
            return False

        ports = {
            _STORAGE_PORT,
            _CQL_PORT,
            _PROMETHEUS_PORT,
            _API_PORT,
            _SHARD_AWARE_CQL_PORT,
        }
        if include_jmx:
            ports.add(_JMX_PORT)
        if http:
            ports.add(_ALTERNATOR_HTTP_PORT)
        if https:
            ports.add(_ALTERNATOR_HTTPS_PORT)

        # Reserve the full nine-node range. Private clusters may add nodes later.
        for node in range(1, _MAXIMUM_NODE_COUNT + 1):
            address = f"127.0.{ccm_id}.{node}"
            for port in ports:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                        probe.bind((address, port))
                except OSError:
                    return False
        return True

    def _reserve_id(self, spec: ClusterSpec, include_jmx: bool) -> _IdLease:
        last_failure: Exception | None = None
        for ccm_id in range(1, 100):
            lock_path = _id_lock_path(self._id_locks_directory, ccm_id)
            owner_path = _id_owner_path(self._id_locks_directory, ccm_id)
            lock_fd: int | None = None
            owner_publication_attempted = False
            try:
                lock_fd = _try_file_lock(lock_path)
                if lock_fd is None:
                    continue
                if _read_reservation_owner(owner_path) is not None:
                    _unlock_and_close(lock_fd)
                    lock_fd = None
                    continue
                if not self.is_address_range_available(spec, ccm_id, include_jmx):
                    _unlock_and_close(lock_fd)
                    lock_fd = None
                    continue
                _delete_reservation_staging_files(self._id_locks_directory, ccm_id)
                owner_publication_attempted = True
                _write_exclusive(owner_path, f"{self._run_name}\n")
                id_lease = _IdLease(ccm_id, owner_path, lock_fd)
                self._retain_incomplete_id_lease(id_lease)
                return id_lease
            except BaseException as exc:
                cleanup_failure: BaseException | None = None
                if owner_publication_attempted:
                    try:
                        if _read_reservation_owner(owner_path) == self._run_name:
                            _unlink_owned_file(owner_path)
                            _fsync_directory(owner_path.parent)
                    except BaseException as exception:
                        cleanup_failure = exception
                if lock_fd is not None:
                    _unlock_and_close(lock_fd)
                    for pending in self._incomplete_id_leases:
                        if pending.ccm_id == ccm_id and pending.lock_fd == lock_fd:
                            pending.lock_fd = -1
                if cleanup_failure is not None:
                    self._incomplete_ownership = True
                    raise CcmRunStateError(
                        f"Cannot roll back CCM ID {ccm_id} reservation"
                    ) from exc
                for pending in tuple(self._incomplete_id_leases):
                    if pending.ccm_id == ccm_id:
                        pending.released = True
                        self._forget_incomplete_id_lease(pending)
                if not isinstance(exc, Exception):
                    raise
                last_failure = exc

        raise CcmRunStateError("No CCM cluster IDs are available") from last_failure

    def _ensure_open(self) -> None:
        if self._closed:
            raise CcmRunStateError("CCM run state is closed")

    def _retain_incomplete_id_lease(self, lease: _IdLease) -> None:
        if all(existing is not lease for existing in self._incomplete_id_leases):
            self._incomplete_id_leases.append(lease)
        self._incomplete_ownership = True

    def _forget_incomplete_id_lease(self, lease: _IdLease) -> None:
        self._incomplete_id_leases = [
            existing for existing in self._incomplete_id_leases if existing is not lease
        ]
        self._incomplete_ownership = bool(self._incomplete_id_leases)

    @classmethod
    def _recover_stale_runs(
        cls,
        runs_directory: Path,
        id_locks_directory: Path,
        cleanup: StaleClusterCleanup,
    ) -> None:
        runs = sorted(
            (
                entry
                for entry in runs_directory.iterdir()
                if _RUN_NAME_RE.fullmatch(entry.name)
            ),
            key=lambda path: path.name,
        )
        for run_directory in runs:
            try:
                cls._recover_stale_run(
                    run_directory,
                    runs_directory,
                    id_locks_directory,
                    cleanup,
                )
            except Exception as exc:
                print(
                    f"Preserving stale CCM run after cleanup failure: {run_directory}",
                    file=sys.stderr,
                )
                traceback.print_exception(exc, file=sys.stderr)

    @classmethod
    def _recover_stale_run(
        cls,
        run_directory: Path,
        runs_directory: Path,
        id_locks_directory: Path,
        cleanup: StaleClusterCleanup,
    ) -> None:
        _validate_child_directory(runs_directory, run_directory, "run directory")
        owner_path = run_directory / "OWNER"
        try:
            owner = _read_owner(owner_path)
        except FileNotFoundError:
            owner = None
        if owner is not None and _owner_is_active(owner):
            return

        _terminate_marked_processes(run_directory)
        manifests_directory = run_directory / "owned"
        cls._cleanup_owned_staging_files(manifests_directory, run_directory)
        manifests = cls._read_manifests(run_directory)
        if len(manifests) > 1:
            raise CcmRunStateError(
                f"CCM run contains more than one physical cluster manifest: {run_directory}"
            )

        for manifest_path, manifest in manifests:
            cls._ensure_stale_reservation(
                id_locks_directory, manifest.ccm_id, run_directory.name
            )
            cleanup(run_directory, manifest)
            cls._retire_cluster_config(run_directory, manifest.instance_id)
            _unlink_owned_file(manifest_path)

        cls._assert_no_unowned_cluster_state(run_directory)
        cls._cleanup_owned_staging_files(manifests_directory, run_directory)
        reservations = cls._reservations_owned_by(
            id_locks_directory, run_directory.name
        )
        _delete_owned_tree(run_directory)
        for ccm_id in reservations:
            cls._release_stale_reservation(
                id_locks_directory, ccm_id, run_directory.name
            )

    @classmethod
    def _read_manifests(cls, run_directory: Path) -> list[tuple[Path, Manifest]]:
        manifests_directory = run_directory / "owned"
        try:
            _validate_child_directory(
                run_directory, manifests_directory, "manifest directory"
            )
        except FileNotFoundError:
            return []

        manifests: list[tuple[Path, Manifest]] = []
        for path in manifests_directory.iterdir():
            if not path.name.endswith(".properties"):
                raise CcmRunStateError(f"Unexpected CCM ownership metadata at {path}")
            values = _read_key_value_file(path, {"format", "instance_id", "ccm_id"})
            _require_format(values, path)
            instance_id = values["instance_id"]
            _validate_instance_id(instance_id)
            if path.name != f"{instance_id}.properties":
                raise CcmRunStateError(
                    f"CCM manifest name does not match instance at {path}"
                )
            ccm_id = _parse_ccm_id(values["ccm_id"], path)
            manifests.append((path, Manifest(instance_id, ccm_id)))
        manifests.sort(key=lambda item: item[0].name)
        return manifests

    @staticmethod
    def _cleanup_owned_staging_files(
        manifests_directory: Path, run_directory: Path
    ) -> None:
        try:
            _validate_child_directory(
                run_directory, manifests_directory, "manifest directory"
            )
        except FileNotFoundError:
            return
        for path in manifests_directory.iterdir():
            if _STAGING_RE.fullmatch(path.name):
                _validate_metadata_file(path, "CCM metadata staging file")
                path.unlink()

    @staticmethod
    def _retire_cluster_config(run_directory: Path, instance_id: str) -> None:
        clusters_directory = run_directory / "clusters"
        try:
            _validate_child_directory(
                run_directory, clusters_directory, "clusters directory"
            )
        except FileNotFoundError:
            return

        cluster_config = clusters_directory / instance_id
        try:
            _validate_child_directory(
                clusters_directory, cluster_config, "cluster config directory"
            )
        except FileNotFoundError:
            return

        for entry in cluster_config.iterdir():
            if entry.name == "tls":
                _validate_child_directory(cluster_config, entry, "TLS directory")
                _validate_owned_tree(entry)
                continue
            if entry.name == "ccm-commands.log" or (
                entry.name.startswith("ccm-command-") and entry.name.endswith(".log")
            ):
                _validate_regular_owned_file(entry, "CCM command log")
                continue
            raise CcmRunStateError(
                f"Unexpected state remains in retired CCM config at {entry}"
            )
        _delete_owned_tree(cluster_config)

    @staticmethod
    def _assert_no_unowned_cluster_state(run_directory: Path) -> None:
        clusters_directory = run_directory / "clusters"
        try:
            _validate_child_directory(
                run_directory, clusters_directory, "clusters directory"
            )
        except FileNotFoundError:
            return
        try:
            next(clusters_directory.iterdir())
        except StopIteration:
            return
        raise CcmRunStateError(
            f"Stale CCM run contains cluster state without valid manifest: {run_directory}"
        )

    @staticmethod
    def _ensure_stale_reservation(
        id_locks_directory: Path, ccm_id: int, expected_owner: str
    ) -> None:
        owner_path = _id_owner_path(id_locks_directory, ccm_id)
        actual_owner = _read_reservation_owner(owner_path)
        if actual_owner == expected_owner:
            return
        if actual_owner is not None:
            raise CcmRunStateError(f"CCM ID {ccm_id} is reserved by different run")

        lock_fd = _try_file_lock(_id_lock_path(id_locks_directory, ccm_id))
        if lock_fd is None:
            raise CcmRunStateError(f"CCM ID {ccm_id} is still locked")
        try:
            actual_owner = _read_reservation_owner(owner_path)
            if actual_owner is None:
                _write_exclusive(owner_path, f"{expected_owner}\n")
            elif actual_owner != expected_owner:
                raise CcmRunStateError(
                    f"CCM ID {ccm_id} changed owner during stale cleanup"
                )
        finally:
            _unlock_and_close(lock_fd)

    @staticmethod
    def _release_stale_reservation(
        id_locks_directory: Path, ccm_id: int, expected_owner: str
    ) -> None:
        owner_path = _id_owner_path(id_locks_directory, ccm_id)
        actual_owner = _read_reservation_owner(owner_path)
        if actual_owner is None:
            return
        if actual_owner != expected_owner:
            raise CcmRunStateError(f"CCM ID {ccm_id} is reserved by different run")

        lock_fd = _try_file_lock(_id_lock_path(id_locks_directory, ccm_id))
        if lock_fd is None:
            raise CcmRunStateError(f"CCM ID {ccm_id} is still locked")
        try:
            actual_owner = _read_reservation_owner(owner_path)
            if actual_owner is not None and actual_owner != expected_owner:
                raise CcmRunStateError(
                    f"CCM ID {ccm_id} changed owner during stale cleanup"
                )
            if actual_owner is not None:
                _unlink_owned_file(owner_path)
                _fsync_directory(owner_path.parent)
        finally:
            _unlock_and_close(lock_fd)

    @staticmethod
    def _reservations_owned_by(id_locks_directory: Path, run_name: str) -> list[int]:
        reservations: list[int] = []
        for ccm_id in range(1, 100):
            owner_path = _id_owner_path(id_locks_directory, ccm_id)
            try:
                owner = _read_reservation_owner(owner_path)
            except Exception:
                print(
                    f"Preserving malformed CCM ID reservation at {owner_path}",
                    file=sys.stderr,
                )
                continue
            if owner == run_name:
                reservations.append(ccm_id)
        return reservations

    @classmethod
    def _release_reservations_for_missing_runs(
        cls, runs_directory: Path, id_locks_directory: Path
    ) -> None:
        for ccm_id in range(1, 100):
            owner_path = _id_owner_path(id_locks_directory, ccm_id)
            try:
                owner = _read_reservation_owner(owner_path)
            except Exception:
                print(
                    f"Preserving malformed CCM ID reservation at {owner_path}",
                    file=sys.stderr,
                )
                continue
            if owner is None:
                continue
            run_directory = runs_directory / owner
            try:
                run_directory.lstat()
            except FileNotFoundError:
                pass
            else:
                continue
            if not cls._address_range_available_for_flags(
                ccm_id, http=True, https=True, include_jmx=True
            ):
                print(
                    "Preserving CCM ID reservation without retired run at "
                    f"{owner_path}",
                    file=sys.stderr,
                )
                continue
            try:
                cls._release_stale_reservation(id_locks_directory, ccm_id, owner)
            except Exception as exc:
                print(
                    f"Preserving CCM ID reservation after release failure: {owner_path}",
                    file=sys.stderr,
                )
                traceback.print_exception(exc, file=sys.stderr)

    @staticmethod
    def _create_run_directory(runs_directory: Path) -> Path:
        for _ in range(10):
            run_name = f"ccm-runtime.{uuid.uuid4().hex}"
            run_directory = runs_directory / run_name
            try:
                run_directory.mkdir(mode=0o700)
            except FileExistsError:
                continue
            try:
                os.chmod(run_directory, 0o700, follow_symlinks=False)
                manifests_directory = run_directory / "owned"
                manifests_directory.mkdir(mode=0o700)
                os.chmod(manifests_directory, 0o700, follow_symlinks=False)
                _write_exclusive(
                    run_directory / "OWNER",
                    "format="
                    f"{_FORMAT_VERSION}\n"
                    f"pid={os.getpid()}\n"
                    f"start_ticks={_read_process_start_ticks(os.getpid())}\n"
                    f"boot_id={_current_boot_id()}\n",
                )
                _fsync_directory(run_directory)
                return run_directory.resolve(strict=True)
            except BaseException:
                with contextlib.suppress(BaseException):
                    _delete_owned_tree(run_directory)
                raise
        raise CcmRunStateError("Unable to allocate unique CCM run directory")


def _require_linux() -> None:
    if not sys.platform.startswith("linux") or not Path("/proc/self/status").exists():
        raise CcmRunStateError("Native scylla-ccm run state supports Linux only")


def _normalize_without_following(path: Path) -> Path:
    expanded = os.path.expanduser(os.fspath(path))
    return Path(os.path.abspath(os.path.normpath(expanded)))


def _prepare_root(requested: Path) -> Path:
    normalized = _normalize_without_following(requested)
    _reject_unsafe_root(normalized)
    _reject_symbolic_link_components(normalized)
    normalized.mkdir(mode=0o700, parents=True, exist_ok=True)
    _validate_directory(normalized, "CCM root")
    real = normalized.resolve(strict=True)
    if real != normalized:
        raise CcmRunStateError(
            f"CCM root must not traverse symbolic links: {normalized}"
        )
    os.chmod(real, 0o700, follow_symlinks=False)
    return real


def _reject_unsafe_root(root: Path) -> None:
    filesystem_root = Path(root.anchor)
    temporary_root = Path(tempfile.gettempdir()).resolve()
    home = Path.home().resolve()
    project = Path.cwd().resolve()
    build_output = project / "target"
    if (
        not root.is_absolute()
        or root == filesystem_root
        or root.parent == filesystem_root
        or root in {temporary_root, Path("/var/tmp"), Path("/dev/shm"), home}
        or _is_relative_to(project, root)
        or _is_relative_to(root, build_output)
    ):
        raise CcmRunStateError(f"Refusing unsafe CCM root: {root}")


def _reject_symbolic_link_components(path: Path) -> None:
    current: Path | None = path
    while current is not None:
        try:
            attributes = current.lstat()
        except FileNotFoundError:
            pass
        else:
            if stat.S_ISLNK(attributes.st_mode):
                raise CcmRunStateError(
                    f"CCM root must not traverse symbolic links: {path}"
                )
        parent = current.parent
        current = None if parent == current else parent


def _prepare_child_directory(root: Path, name: str) -> Path:
    child = root / name
    with contextlib.suppress(FileExistsError):
        child.mkdir(mode=0o700)
    _validate_child_directory(root, child, name)
    os.chmod(child, 0o700, follow_symlinks=False)
    return child.resolve(strict=True)


def _validate_child_directory(parent: Path, child: Path, description: str) -> None:
    if child.parent != parent:
        raise CcmRunStateError(f"Unsafe CCM {description} at {child}")
    try:
        _validate_directory(child, f"CCM {description}")
    except FileNotFoundError:
        raise
    if child.resolve(strict=True) != child:
        raise CcmRunStateError(f"Unsafe CCM {description} at {child}")


def _validate_directory(path: Path, description: str) -> None:
    attributes = path.lstat()
    if stat.S_ISLNK(attributes.st_mode) or not stat.S_ISDIR(attributes.st_mode):
        raise CcmRunStateError(f"{description} is not safe directory: {path}")
    _require_current_user(attributes, path, description)


def _require_current_user(
    attributes: os.stat_result, path: Path, description: str
) -> None:
    if attributes.st_uid not in {os.getuid(), os.geteuid()}:
        raise CcmRunStateError(f"{description} is not owned by current user: {path}")


def _validate_regular_owned_file(path: Path, description: str) -> os.stat_result:
    attributes = path.lstat()
    if stat.S_ISLNK(attributes.st_mode) or not stat.S_ISREG(attributes.st_mode):
        raise CcmRunStateError(f"Unsafe {description} at {path}")
    _require_current_user(attributes, path, description)
    return attributes


def _validate_metadata_file(path: Path, description: str) -> os.stat_result:
    attributes = _validate_regular_owned_file(path, description)
    if attributes.st_size > _MAXIMUM_METADATA_BYTES:
        raise CcmRunStateError(f"Oversized {description} at {path}")
    return attributes


def _validate_owned_tree(root: Path) -> None:
    attributes = root.lstat()
    if stat.S_ISLNK(attributes.st_mode) or not stat.S_ISDIR(attributes.st_mode):
        raise CcmRunStateError(f"Unsafe CCM-owned directory at {root}")
    _require_current_user(attributes, root, "CCM-owned directory")
    for entry in root.iterdir():
        attributes = entry.lstat()
        if stat.S_ISLNK(attributes.st_mode):
            raise CcmRunStateError(f"Unsafe symbolic link in CCM-owned tree: {entry}")
        _require_current_user(attributes, entry, "CCM-owned entry")
        if stat.S_ISDIR(attributes.st_mode):
            _validate_owned_tree(entry)
        elif not stat.S_ISREG(attributes.st_mode):
            raise CcmRunStateError(f"Unsafe CCM-owned entry at {entry}")


def _delete_owned_tree(root: Path) -> None:
    _validate_owned_tree(root)
    for entry in list(root.iterdir()):
        attributes = entry.lstat()
        if stat.S_ISDIR(attributes.st_mode):
            _delete_owned_tree(entry)
        else:
            _unlink_owned_file(entry)
    root.rmdir()


def _unlink_owned_file(path: Path, *, missing_ok: bool = False) -> None:
    try:
        _validate_regular_owned_file(path, "CCM-owned file")
    except FileNotFoundError:
        if missing_ok:
            return
        raise
    path.unlink()


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _open_lock_fd(path: Path) -> int:
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as exc:
        raise CcmRunStateError(f"Unsafe CCM lock file at {path}") from exc
    try:
        attributes = os.fstat(fd)
        if not stat.S_ISREG(attributes.st_mode):
            raise CcmRunStateError(f"Unsafe CCM lock file at {path}")
        _require_current_user(attributes, path, "CCM lock file")
        os.fchmod(fd, 0o600)
        return fd
    except BaseException:
        os.close(fd)
        raise


@contextlib.contextmanager
def _file_lock(path: Path) -> Iterator[None]:
    with _SCAN_MUTEX:
        fd = _open_lock_fd(path)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            _unlock_and_close(fd)


def _try_file_lock(path: Path) -> int | None:
    fd = _open_lock_fd(path)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException as exc:
        os.close(fd)
        if isinstance(exc, OSError) and exc.errno in {errno.EACCES, errno.EAGAIN}:
            return None
        raise
    return fd


def _unlock_and_close(fd: int) -> None:
    if fd < 0:
        return
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _id_lock_path(id_locks_directory: Path, ccm_id: int) -> Path:
    return id_locks_directory / f"{ccm_id}.lock"


def _id_owner_path(id_locks_directory: Path, ccm_id: int) -> Path:
    return id_locks_directory / f"{ccm_id}.owner"


def _read_reservation_owner(owner_path: Path) -> str | None:
    try:
        _validate_metadata_file(owner_path, "CCM ID reservation")
    except FileNotFoundError:
        return None
    try:
        value = owner_path.read_text(encoding="ascii")
    except (OSError, UnicodeError) as exc:
        raise CcmRunStateError(
            f"Unreadable CCM ID reservation at {owner_path}"
        ) from exc
    if not value.endswith("\n") or not _RUN_NAME_RE.fullmatch(value[:-1]):
        raise CcmRunStateError(f"Malformed CCM ID reservation at {owner_path}")
    return value[:-1]


def _delete_reservation_staging_files(id_locks_directory: Path, ccm_id: int) -> None:
    prefix = f".owner-{ccm_id}.owner-"
    for path in id_locks_directory.iterdir():
        if (
            path.name.startswith(prefix)
            and path.name.endswith(".tmp")
            and _STAGING_RE.fullmatch(path.name)
        ):
            _validate_metadata_file(path, "CCM ID reservation staging file")
            path.unlink()


def _write_exclusive(target: Path, contents: str) -> None:
    try:
        encoded = contents.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError("CCM metadata must be ASCII") from exc
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".owner-{target.name}-", suffix=".tmp", dir=target.parent
    )
    temporary = Path(temporary_name)
    published = False
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb", closefd=True) as stream:
            fd = -1
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, target, follow_symlinks=False)
        published = True
        _fsync_directory(target.parent)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            if not published:
                raise


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(directory, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _read_key_value_file(path: Path, expected_keys: AbstractSet[str]) -> dict[str, str]:
    _validate_metadata_file(path, "CCM metadata")
    try:
        text = path.read_text(encoding="ascii")
    except (OSError, UnicodeError) as exc:
        raise CcmRunStateError(f"Unreadable CCM metadata at {path}") from exc
    if not text.endswith("\n"):
        raise CcmRunStateError(f"Malformed CCM metadata at {path}")

    values: dict[str, str] = {}
    for line in text[:-1].split("\n"):
        key, separator, value = line.partition("=")
        if (
            not separator
            or not key
            or not value
            or key not in expected_keys
            or key in values
        ):
            raise CcmRunStateError(f"Malformed CCM metadata at {path}")
        values[key] = value
    if set(values) != set(expected_keys):
        raise CcmRunStateError(f"Incomplete CCM metadata at {path}")
    return values


def _require_format(values: dict[str, str], path: Path) -> None:
    if values.get("format") != str(_FORMAT_VERSION):
        raise CcmRunStateError(f"Unsupported CCM metadata format at {path}")


def _parse_ccm_id(value: str, path: Path) -> int:
    try:
        ccm_id = int(value)
    except ValueError as exc:
        raise CcmRunStateError(f"Malformed CCM ID at {path}") from exc
    if not 1 <= ccm_id < 100 or str(ccm_id) != value:
        raise CcmRunStateError(f"Malformed CCM ID at {path}")
    return ccm_id


def _validate_instance_id(instance_id: str) -> None:
    if not isinstance(instance_id, str) or not _INSTANCE_ID_RE.fullmatch(instance_id):
        raise ValueError(f"Unsafe CCM cluster instance ID: {instance_id}")


def _read_owner(owner_path: Path) -> _Owner:
    values = _read_key_value_file(
        owner_path, {"format", "pid", "start_ticks", "boot_id"}
    )
    _require_format(values, owner_path)
    try:
        pid = int(values["pid"])
        start_ticks = int(values["start_ticks"])
    except ValueError as exc:
        raise CcmRunStateError(f"Malformed CCM owner metadata at {owner_path}") from exc
    boot_id = values["boot_id"]
    if (
        pid < 1
        or start_ticks < 1
        or str(pid) != values["pid"]
        or str(start_ticks) != values["start_ticks"]
        or not _BOOT_ID_RE.fullmatch(boot_id)
    ):
        raise CcmRunStateError(f"Malformed CCM owner metadata at {owner_path}")
    return _Owner(pid, start_ticks, boot_id)


def _owner_is_active(owner: _Owner) -> bool:
    if owner.boot_id != _current_boot_id():
        return False
    try:
        state, start_ticks = _read_process_state_and_start_ticks(owner.pid)
        return state != "Z" and start_ticks == owner.start_ticks
    except (FileNotFoundError, ProcessLookupError):
        return False


def _current_boot_id() -> str:
    value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    if not _BOOT_ID_RE.fullmatch(value):
        raise CcmRunStateError("Unable to determine Linux boot identity")
    return value


def _read_process_start_ticks(pid: int) -> int:
    return _read_process_state_and_start_ticks(pid)[1]


def _read_process_state_and_start_ticks(pid: int) -> tuple[str, int]:
    stat_text = Path("/proc").joinpath(str(pid), "stat").read_text(encoding="ascii")
    command_end = stat_text.rfind(")")
    if command_end < 0 or command_end + 2 >= len(stat_text):
        raise CcmRunStateError(f"Unable to parse process identity for PID {pid}")
    fields = stat_text[command_end + 2 :].split()
    if len(fields) <= 19:
        raise CcmRunStateError(f"Unable to parse process identity for PID {pid}")
    try:
        start_ticks = int(fields[19])
    except ValueError as exc:
        raise CcmRunStateError(
            f"Unable to parse process identity for PID {pid}"
        ) from exc
    if start_ticks < 1:
        raise CcmRunStateError(f"Unable to parse process identity for PID {pid}")
    return fields[0], start_ticks


def _read_process_uids(status_path: Path) -> tuple[int, int]:
    uid_lines = [
        line.removeprefix("Uid:").strip()
        for line in status_path.read_text(encoding="ascii").splitlines()
        if line.startswith("Uid:")
    ]
    if len(uid_lines) != 1:
        raise CcmRunStateError(f"Malformed process identity at {status_path}")
    values = uid_lines[0].split()
    if len(values) != 4 or any(
        not value.isascii() or not value.isdigit() for value in values
    ):
        raise CcmRunStateError(f"Malformed process identity at {status_path}")
    return int(values[0]), int(values[1])


def _shares_current_identity(real_uid: int, effective_uid: int) -> bool:
    current = {os.getuid(), os.geteuid()}
    return real_uid in current or effective_uid in current


def _contains_run_path_argument(command_line: bytes, run_path: str) -> bool:
    prefix = f"{run_path}{os.sep}"
    for raw_argument in command_line.split(b"\0"):
        argument = raw_argument.decode("utf-8", errors="surrogateescape")
        if argument == run_path or argument.startswith(prefix):
            return True
    return False


def _contains_environment_entry(environment: bytes, expected: str) -> bool:
    expected_bytes = expected.encode("utf-8")
    return expected_bytes in environment.split(b"\0")


def _marked_processes(run_directory: Path) -> list[_MarkedProcess]:
    run_path = os.fspath(run_directory.resolve(strict=True))
    expected_marker = f"SCYLLA_CCM_RUN_DIR={run_path}"
    current_pid = os.getpid()
    result: list[_MarkedProcess] = []

    for process_directory in Path("/proc").iterdir():
        if not process_directory.name.isascii() or not process_directory.name.isdigit():
            continue
        pid = int(process_directory.name)
        try:
            real_uid, effective_uid = _read_process_uids(process_directory / "status")
            if not _shares_current_identity(real_uid, effective_uid):
                continue
            start_ticks = _read_process_start_ticks(pid)
            try:
                command_references_run = _contains_run_path_argument(
                    (process_directory / "cmdline").read_bytes(), run_path
                )
            except PermissionError:
                command_references_run = False
            try:
                environment = (process_directory / "environ").read_bytes()
            except PermissionError as exc:
                if command_references_run:
                    raise CcmRunStateError(
                        "Cannot prove ownership of process "
                        f"{pid} whose command references stale CCM run {run_directory}"
                    ) from exc
                continue
            if not _contains_environment_entry(environment, expected_marker):
                continue
            if _read_process_start_ticks(pid) != start_ticks:
                continue
            if pid == current_pid:
                raise CcmRunStateError(
                    "Current process carries supposedly stale CCM run marker"
                )
            result.append(_MarkedProcess(pid, start_ticks))
        except (FileNotFoundError, ProcessLookupError):
            continue
    return result


def _signal_marked_process(process: _MarkedProcess, signal_number: int) -> None:
    try:
        if _read_process_start_ticks(process.pid) != process.start_ticks:
            return
        os.kill(process.pid, signal_number)
    except (FileNotFoundError, ProcessLookupError):
        return


def _wait_for_marked_processes(run_directory: Path, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _marked_processes(run_directory):
            return True
        time.sleep(_PROCESS_POLL_SECONDS)
    return not _marked_processes(run_directory)


def _terminate_marked_processes(run_directory: Path) -> None:
    processes = _marked_processes(run_directory)
    for process in processes:
        _signal_marked_process(process, signal.SIGTERM)
    if _wait_for_marked_processes(run_directory, _TERM_GRACE_SECONDS):
        return
    processes = _marked_processes(run_directory)
    for process in processes:
        _signal_marked_process(process, signal.SIGKILL)
    if not _wait_for_marked_processes(run_directory, _KILL_GRACE_SECONDS):
        raise CcmRunStateError(
            "Processes carrying stale CCM run marker did not terminate"
        )


def _transport_names(spec: object) -> set[str]:
    transports = getattr(spec, "transports", ())
    if callable(transports):
        transports = transports()
    names: set[str] = set()
    for transport in transports:
        name = getattr(transport, "name", str(transport))
        names.add(str(name).rsplit(".", 1)[-1].upper())
    return names


__all__ = [
    "CcmRunState",
    "CcmRunStateError",
    "ClusterHandle",
    "Manifest",
    "StaleClusterCleanup",
]
