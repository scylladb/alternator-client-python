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

"""Single-physical-cluster pool matching alternator-client-java CCM semantics."""

from __future__ import annotations

import atexit
import os
import sys
import threading
import traceback
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from tests.testinfra.ccm_provisioner import CcmProvisioner
from tests.testinfra.cluster import (
    PhysicalTestCluster,
    PrivateClusterLease,
    ReusableClusterLease,
    TestResourceScope,
)
from tests.testinfra.cluster_spec import ClusterSpec
from tests.testinfra.run_state import CcmRunState, ClusterHandle, Manifest

MAXIMUM_NODE_COUNT = 9
_PROCESS_INSTANCE_TOKEN = uuid.uuid4().hex[:16]


ResourceCleanup = Callable[[TestResourceScope], None]


@dataclass(frozen=True, slots=True)
class _PooledCluster:
    """Opaque generation token rejecting stale or duplicate lease release."""

    generation: int


@dataclass(frozen=True, slots=True)
class _Ownership:
    ccm_id: int
    durable_handle: ClusterHandle | None


@dataclass(slots=True)
class _Slot:
    token: _PooledCluster
    reuse_key: str | None
    private_cluster: bool
    cluster: PhysicalTestCluster
    ownership: _Ownership
    active_resources: set[TestResourceScope] = field(default_factory=set)
    poisoned: bool = False
    retiring: bool = False


class TestClusterPool:
    """Deliberately small one-cluster-slot test harness."""

    __test__ = False
    _instance_counter = 0
    _counter_lock = threading.Lock()

    def __init__(
        self,
        provisioner: CcmProvisioner,
        maximum_nodes: int,
        cleanup_resources: ResourceCleanup | None = None,
        run_state: CcmRunState | None = None,
    ) -> None:
        if maximum_nodes < 1 or maximum_nodes > MAXIMUM_NODE_COUNT:
            raise ValueError(
                f"maximum_nodes must be between 1 and {MAXIMUM_NODE_COUNT}"
            )
        self._provisioner = provisioner
        self._maximum_nodes = maximum_nodes
        self._cleanup_resources = cleanup_resources or self._cleanup_scope
        self._run_state = run_state
        self._condition = threading.Condition(threading.RLock())
        self._lease_counter = 0
        self._generation = 0
        self._current: _Slot | None = None
        self._terminal_failure: BaseException | None = None
        self._closed = False

    @classmethod
    def create_default(cls) -> TestClusterPool:
        """Open durable state, recover stale runs, and create process pool."""

        def cleanup_stale(run_directory: Path, manifest: Manifest) -> None:
            provisioner = CcmProvisioner(run_directory)
            provisioner.cleanup_stale_cluster(
                manifest.instance_id,
                manifest.ccm_id,
                run_directory / "clusters" / manifest.instance_id,
            )

        state = CcmRunState.open_default(cleanup_stale)
        try:
            provisioner = CcmProvisioner(state.run_directory)
            configured_maximum = _parse_positive_integer(
                "SCYLLA_CCM_MAX_NODES", MAXIMUM_NODE_COUNT
            )
            return cls(
                provisioner,
                min(configured_maximum, MAXIMUM_NODE_COUNT),
                run_state=state,
            )
        except BaseException:
            state.close()
            raise

    def acquire_reusable(self, spec: ClusterSpec) -> ReusableClusterLease:
        """Acquire shared read-only access or replace an idle incompatible slot."""
        with self._condition:
            self._validate_demand(spec)
            self._throw_if_unavailable()
            current = self._current
            if current is not None:
                if current.private_cluster:
                    raise RuntimeError(
                        "Private CCM cluster already active in this process"
                    )
                if current.active_resources:
                    if current.reuse_key != spec.reuse_key or current.poisoned:
                        raise RuntimeError(
                            "Active reusable CCM cluster is incompatible with "
                            "requested specification"
                        )
                    return self._reusable_lease(current)

                if (
                    current.reuse_key == spec.reuse_key
                    and not current.poisoned
                    and not current.cluster.is_dirty
                    and self._provisioner.is_healthy(current.cluster)
                ):
                    return self._reusable_lease(current)
                self._retire_current_locked()

            created = self._provision_locked(spec, private_cluster=False)
            return self._reusable_lease(created)

    def provision_private(self, spec: ClusterSpec) -> PrivateClusterLease:
        """Provision exclusive cluster with process and topology controls."""
        with self._condition:
            self._validate_demand(spec)
            self._throw_if_unavailable()
            current = self._current
            if current is not None:
                if current.private_cluster or current.active_resources:
                    raise RuntimeError(
                        "CCM cluster lease already active in this process"
                    )
                self._retire_current_locked()

            created = self._provision_locked(spec, private_cluster=True)
            return PrivateClusterLease(
                self, created.cluster, self._create_resource_scope(created.cluster)
            )

    def release_reusable(self, token: object, resources: TestResourceScope) -> None:
        """Clean one namespace and retire poisoned or closing shared state."""
        with self._condition:
            current = self._current
            if current is None or current.token is not token:
                return
            if resources not in current.active_resources:
                return

            failure: BaseException | None = None
            if not self._closed:
                try:
                    self._cleanup_resources(resources)
                except BaseException as exception:
                    current.poisoned = True
                    current.cluster.mark_dirty()
                    self._terminal_failure = exception
                    failure = exception

            current.active_resources.discard(resources)
            if (
                not current.active_resources
                and (current.poisoned or self._closed)
                and not current.retiring
            ):
                try:
                    self._retire_current_locked()
                except BaseException as exception:
                    failure = _combine(failure, exception)
            if failure is not None:
                raise failure

    def release_private(self, cluster: PhysicalTestCluster) -> None:
        """Remove exclusive cluster; failed close remains retryable."""
        self._retire_cluster(cluster)

    def reserve_additional_private_node(self, cluster: PhysicalTestCluster) -> None:
        """Enforce configured node ceiling before private expansion."""
        with self._condition:
            self._throw_if_closed()
            current = self._current
            if (
                current is None
                or not current.private_cluster
                or current.cluster is not cluster
            ):
                raise RuntimeError("Private cluster is no longer owned by this pool")
            if len(cluster.nodes) >= self._maximum_nodes:
                raise RuntimeError(
                    f"Adding node would exceed this run's "
                    f"{self._maximum_nodes}-node limit"
                )

    def release_additional_private_node(self, cluster: PhysicalTestCluster) -> None:
        """Accept node removal; membership is authoritative in single-slot model."""

    def close(self) -> None:
        """Remove current cluster and close durable run state."""
        with self._condition:
            self._closed = True
            current = self._current

        failure: BaseException | None = None
        if current is not None:
            try:
                self._retire_cluster(current.cluster)
            except BaseException as exception:
                failure = exception
        if failure is None and self._run_state is not None:
            try:
                self._run_state.close()
            except BaseException as exception:
                failure = exception
        if failure is not None:
            raise failure

    def __enter__(self) -> TestClusterPool:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback_object: object,
    ) -> None:
        self.close()

    @property
    def run_directory(self) -> Path:
        """Return current process run directory."""
        return self._provisioner.run_directory

    def _retire_cluster(self, cluster: PhysicalTestCluster) -> None:
        current: _Slot | None = None
        try:
            with self._condition:
                while (
                    self._current is not None
                    and self._current.cluster is cluster
                    and self._current.retiring
                ):
                    self._condition.wait()
                candidate = self._current
                if candidate is None or candidate.cluster is not cluster:
                    return
                if not candidate.private_cluster and not self._closed:
                    raise RuntimeError("Cluster is not privately owned by this pool")
                current = candidate
                current.retiring = True

            cluster.remove_physical()
            self._complete_ownership(current.ownership)
        except BaseException as exception:
            if current is not None:
                with self._condition:
                    self._terminal_failure = exception
            raise
        else:
            with self._condition:
                if self._current is current:
                    self._current = None
                self._terminal_failure = None
        finally:
            if current is not None:
                with self._condition:
                    current.retiring = False
                    self._condition.notify_all()

    def _retire_current_locked(self) -> None:
        current = self._current
        if current is None:
            return
        try:
            current.cluster.remove_physical()
            self._complete_ownership(current.ownership)
        except BaseException as exception:
            self._terminal_failure = exception
            raise
        self._current = None
        self._terminal_failure = None

    def _provision_locked(self, spec: ClusterSpec, *, private_cluster: bool) -> _Slot:
        instance_id = self._create_instance_id()
        ownership: _Ownership | None = None
        cluster: PhysicalTestCluster | None = None
        handed_off_cluster: PhysicalTestCluster | None = None
        published: _Slot | None = None

        def publish(provisioned: PhysicalTestCluster) -> None:
            nonlocal handed_off_cluster, published
            handed_off_cluster = provisioned
            if ownership is None:
                raise RuntimeError("CCM cluster was provisioned without ownership")
            self._generation += 1
            published = _Slot(
                token=_PooledCluster(self._generation),
                reuse_key=None if private_cluster else spec.reuse_key,
                private_cluster=private_cluster,
                cluster=provisioned,
                ownership=ownership,
            )
            self._current = published

        try:
            ownership = self._reserve_ownership(spec, instance_id)
            cluster = self._provisioner.provision(
                spec,
                instance_id,
                ownership.ccm_id,
                on_provisioned=publish,
            )
            if published is None or handed_off_cluster is not cluster:
                raise RuntimeError("CCM provisioner did not publish its cluster")
            return published
        except BaseException as exception:
            failed_cluster: object = getattr(exception, "cluster", None)
            if not isinstance(failed_cluster, PhysicalTestCluster):
                failed_cluster = handed_off_cluster or cluster
            if not isinstance(failed_cluster, PhysicalTestCluster):
                current = self._current
                if current is not None and current.ownership is ownership:
                    failed_cluster = current.cluster
            if isinstance(failed_cluster, PhysicalTestCluster):
                if ownership is None:
                    raise RuntimeError(
                        "CCM provisioner returned a cluster without ownership"
                    ) from exception
                failed_cluster.mark_dirty()
                if getattr(exception, "recovery_required", False):
                    failed_cluster.mark_recovery_required()
                if self._current is None or self._current.cluster is not failed_cluster:
                    self._generation += 1
                    self._current = _Slot(
                        token=_PooledCluster(self._generation),
                        reuse_key=None,
                        private_cluster=True,
                        cluster=failed_cluster,
                        ownership=ownership,
                        poisoned=True,
                    )
                else:
                    self._current.reuse_key = None
                    self._current.private_cluster = True
                    self._current.poisoned = True
                self._terminal_failure = exception
            else:
                try:
                    if ownership is not None:
                        self._complete_ownership(ownership)
                    elif self._run_state is not None:
                        self._run_state.cancel_cluster(instance_id)
                except BaseException as cleanup_exception:
                    self._terminal_failure = cleanup_exception
                    exception.__context__ = cleanup_exception
            raise

    def _reserve_ownership(self, spec: ClusterSpec, instance_id: str) -> _Ownership:
        include_jmx = self._provisioner.requires_jmx_port_reservation(spec)
        if self._run_state is not None:
            handle = self._run_state.begin_cluster(
                instance_id, spec, include_jmx=include_jmx
            )
            return _Ownership(handle.ccm_id, handle)
        for ccm_id in range(1, 100):
            if CcmRunState.is_address_range_available(
                spec, ccm_id, include_jmx=include_jmx
            ):
                return _Ownership(ccm_id, None)
        raise RuntimeError("No CCM cluster IDs are available")

    def _complete_ownership(self, ownership: _Ownership) -> None:
        if self._run_state is not None and ownership.durable_handle is not None:
            self._run_state.complete_cluster(ownership.durable_handle)

    def _reusable_lease(self, slot: _Slot) -> ReusableClusterLease:
        resources: TestResourceScope | None = None
        try:
            resources = self._create_resource_scope(slot.cluster)
            slot.active_resources.add(resources)
            return ReusableClusterLease(
                self,
                slot.token,
                slot.cluster,
                resources,
            )
        except BaseException:
            if resources is not None:
                slot.active_resources.discard(resources)
            raise

    def _create_resource_scope(self, cluster: PhysicalTestCluster) -> TestResourceScope:
        self._lease_counter += 1
        run_id = os.path.basename(os.fspath(self._provisioner.run_directory))
        return TestResourceScope(cluster, run_id, self._lease_counter)

    def _validate_demand(self, spec: ClusterSpec) -> None:
        if spec.topology.node_count > self._maximum_nodes:
            raise RuntimeError(
                f"Requested cluster exceeds this run's {self._maximum_nodes}-node limit"
            )

    def _throw_if_unavailable(self) -> None:
        self._throw_if_closed()
        if self._terminal_failure is not None:
            raise RuntimeError(
                "CCM pool retained failed cleanup state; close it before "
                "provisioning again"
            ) from self._terminal_failure

    def _throw_if_closed(self) -> None:
        if self._closed:
            raise RuntimeError("CCM cluster pool is closed")

    @classmethod
    def _create_instance_id(cls) -> str:
        with cls._counter_lock:
            cls._instance_counter += 1
            counter = cls._instance_counter
        return f"alternator-python-{os.getpid()}-{_PROCESS_INSTANCE_TOKEN}-{counter}"

    @staticmethod
    def _cleanup_scope(resources: TestResourceScope) -> None:
        resources.cleanup()


def _parse_positive_integer(variable: str, default: int) -> int:
    value = os.environ.get(variable)
    if value is None or value == "":
        return default
    if not value.isascii() or not value.isdecimal() or int(value) < 1:
        raise RuntimeError(f"{variable} must be a positive integer")
    return int(value)


def _combine(first: BaseException | None, second: BaseException) -> BaseException:
    if first is None:
        return second
    first.__context__ = second
    return first


class TestClusters:
    """Process-wide entry point for native CCM cluster leases."""

    __test__ = False
    _lock = threading.RLock()
    _shared_pool: TestClusterPool | None = None

    @classmethod
    def acquire_reusable(cls, spec: ClusterSpec) -> ReusableClusterLease:
        """Acquire reusable cluster from process-wide pool."""
        return cls._pool().acquire_reusable(spec)

    @classmethod
    def provision_private(cls, spec: ClusterSpec) -> PrivateClusterLease:
        """Provision private cluster from process-wide pool."""
        return cls._pool().provision_private(spec)

    @classmethod
    def close_all(cls) -> None:
        """Close process pool and forget it only after successful cleanup."""
        with cls._lock:
            if cls._shared_pool is not None:
                cls._shared_pool.close()
                cls._shared_pool = None

    @classmethod
    def _pool(cls) -> TestClusterPool:
        with cls._lock:
            if cls._shared_pool is None:
                cls._shared_pool = TestClusterPool.create_default()
            return cls._shared_pool


def _close_at_exit() -> None:
    try:
        TestClusters.close_all()
    except BaseException:
        print("Failed to remove CCM cluster during Python shutdown", file=sys.stderr)
        traceback.print_exc()


atexit.register(_close_at_exit)


__all__ = ["MAXIMUM_NODE_COUNT", "TestClusterPool", "TestClusters"]
