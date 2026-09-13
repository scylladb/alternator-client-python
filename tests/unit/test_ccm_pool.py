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

"""Focused tests for Java-compatible single-slot CCM pool behavior."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, NoReturn, cast

import pytest

from tests.testinfra.ccm_provisioner import (
    CcmClusterProvisioningError,
    CcmProvisioner,
)
from tests.testinfra.cluster import (
    PhysicalTestCluster,
    TestClusterNode,
    TestResourceScope,
)
from tests.testinfra.cluster_spec import (
    AlternatorTransport,
    ClusterSpec,
    ClusterTopology,
)
from tests.testinfra.pool import TestClusterPool, _parse_positive_integer
from tests.testinfra.run_state import CcmRunState


class FakeProvisioner:
    """In-memory provisioner recording pool lifecycle calls."""

    def __init__(self, run_directory: Path) -> None:
        self.run_directory = run_directory
        self.clusters_directory = run_directory / "clusters"
        self.provision_count = 0
        self.remove_count = 0
        self.health_count = 0
        self.healthy = True
        self.fail_remove = False
        self.fail_provision_ambiguously = False
        self.interrupt_after_publication = False

    def requires_jmx_port_reservation(self, spec: ClusterSpec) -> bool:
        return False

    def provision(
        self,
        spec: ClusterSpec,
        instance_id: str,
        ccm_id: int,
        *,
        on_provisioned: Callable[[PhysicalTestCluster], None] | None = None,
    ) -> PhysicalTestCluster:
        self.provision_count += 1
        nodes = [
            TestClusterNode(f"node{index}", f"127.0.{ccm_id}.{index}", "dc1", "RAC1")
            for index in range(1, spec.topology.node_count + 1)
        ]
        cluster = PhysicalTestCluster(
            cast(CcmProvisioner, cast(Any, self)),
            instance_id,
            ccm_id,
            self.clusters_directory / instance_id,
            spec,
            nodes,
            None,
            None,
        )
        if self.fail_provision_ambiguously:
            raise CcmClusterProvisioningError(
                cluster, KeyboardInterrupt(), OSError("rollback failed")
            )
        if on_provisioned is not None:
            on_provisioned(cluster)
        if self.interrupt_after_publication:
            raise KeyboardInterrupt
        return cluster

    def is_healthy(self, cluster: PhysicalTestCluster) -> bool:
        self.health_count += 1
        return self.healthy

    def remove(self, cluster: PhysicalTestCluster) -> None:
        self.remove_count += 1
        if self.fail_remove:
            self.fail_remove = False
            raise RuntimeError("injected removal failure")


@pytest.fixture(autouse=True)
def available_address_range(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid real port probes in pool-only tests."""
    monkeypatch.setattr(
        CcmRunState,
        "is_address_range_available",
        staticmethod(lambda _spec, _ccm_id, *_args, **_kwargs: True),
    )


def _spec(identity: str = "one", nodes: int = 1) -> ClusterSpec:
    return (
        ClusterSpec()
        .with_topology(ClusterTopology.single_datacenter(nodes))
        .with_transports(AlternatorTransport.HTTP)
        .with_yaml_override("cluster_identity", identity)
    )


def _pool(
    tmp_path: Path,
    *,
    maximum_nodes: int = 3,
    cleanup: Callable[[TestResourceScope], None] | None = None,
) -> tuple[TestClusterPool, FakeProvisioner]:
    provisioner = FakeProvisioner(tmp_path)
    pool = TestClusterPool(
        cast(CcmProvisioner, cast(Any, provisioner)),
        maximum_nodes,
        cleanup_resources=cleanup or (lambda _resources: None),
    )
    return pool, provisioner


def test_matching_leases_share_cluster_and_have_distinct_resources(
    tmp_path: Path,
) -> None:
    pool, provisioner = _pool(tmp_path)
    with pool:
        first = pool.acquire_reusable(_spec())
        second = pool.acquire_reusable(_spec())
        assert first.cluster.instance_id == second.cluster.instance_id
        assert first.resources.prefix != second.resources.prefix
        assert not hasattr(first.cluster, "start")
        assert provisioner.provision_count == 1
        first.close()
        second.close()
        with pool.acquire_reusable(_spec()) as reused:
            assert reused.cluster.instance_id == first.cluster.instance_id
        assert provisioner.health_count == 1


def test_active_incompatible_and_private_requests_fail_fast(tmp_path: Path) -> None:
    pool, provisioner = _pool(tmp_path)
    with pool:
        active = pool.acquire_reusable(_spec("first"))
        with pytest.raises(RuntimeError, match="incompatible"):
            pool.acquire_reusable(_spec("second"))
        with pytest.raises(RuntimeError, match="already active"):
            pool.provision_private(_spec("first"))
        assert provisioner.remove_count == 0
        active.close()


def test_idle_incompatible_and_unhealthy_clusters_are_replaced(
    tmp_path: Path,
) -> None:
    pool, provisioner = _pool(tmp_path)
    with pool:
        with pool.acquire_reusable(_spec("first")):
            pass
        with pool.acquire_reusable(_spec("second")):
            pass
        assert provisioner.provision_count == 2
        assert provisioner.remove_count == 1

        provisioner.healthy = False
        with pool.acquire_reusable(_spec("second")):
            pass
        assert provisioner.provision_count == 3
        assert provisioner.remove_count == 2


def test_cleanup_failure_is_reported_and_poisoned_cluster_removed(
    tmp_path: Path,
) -> None:
    def fail_cleanup(resources: object) -> None:
        raise RuntimeError("injected resource cleanup failure")

    pool, provisioner = _pool(tmp_path, cleanup=fail_cleanup)
    lease = pool.acquire_reusable(_spec())
    with pytest.raises(RuntimeError, match="resource cleanup"):
        lease.close()
    assert provisioner.remove_count == 1
    pool.close()


def test_failed_private_close_is_retryable_and_blocks_new_work(
    tmp_path: Path,
) -> None:
    pool, provisioner = _pool(tmp_path)
    lease = pool.provision_private(_spec())
    provisioner.fail_remove = True

    with pytest.raises(RuntimeError, match="removal failure"):
        lease.close()
    with pytest.raises(RuntimeError, match="failed cleanup state"):
        pool.acquire_reusable(_spec())

    lease.close()
    lease.close()
    assert provisioner.remove_count == 2
    pool.close()


def test_private_lease_is_exclusive_and_honors_lower_node_limit(
    tmp_path: Path,
) -> None:
    pool, _provisioner = _pool(tmp_path, maximum_nodes=1)
    with pool:
        with pytest.raises(RuntimeError, match="exceeds"):
            pool.provision_private(_spec(nodes=2))
        with pool.provision_private(_spec()) as private:
            with pytest.raises(RuntimeError, match="already active"):
                pool.acquire_reusable(_spec())
            with pytest.raises(RuntimeError, match="1-node limit"):
                private.control.add_node("dc1", "RAC1")


def test_pool_close_removes_active_reusable_once(tmp_path: Path) -> None:
    pool, provisioner = _pool(tmp_path)
    lease = pool.acquire_reusable(_spec())
    pool.close()
    pool.close()
    lease.close()
    assert provisioner.remove_count == 1


def test_successful_provision_is_retained_if_return_handoff_is_interrupted(
    tmp_path: Path,
) -> None:
    pool, provisioner = _pool(tmp_path)
    provisioner.interrupt_after_publication = True

    with pytest.raises(KeyboardInterrupt):
        pool.provision_private(_spec())

    assert provisioner.provision_count == 1
    assert provisioner.remove_count == 0
    pool.close()
    assert provisioner.remove_count == 1


def test_interrupted_ownership_return_is_cancelled_by_instance_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "state"
    state = CcmRunState.open(root, lambda _run, _manifest: None)
    run_directory = state.run_directory
    provisioner = FakeProvisioner(run_directory)
    pool = TestClusterPool(
        cast(CcmProvisioner, cast(Any, provisioner)),
        maximum_nodes=1,
        cleanup_resources=lambda _resources: None,
        run_state=state,
    )
    reserve_ownership = pool._reserve_ownership  # noqa: SLF001 -- injection seam

    def interrupt_after_reservation(spec: ClusterSpec, instance_id: str) -> NoReturn:
        reserve_ownership(spec, instance_id)
        raise KeyboardInterrupt

    monkeypatch.setattr(pool, "_reserve_ownership", interrupt_after_reservation)

    with pytest.raises(KeyboardInterrupt):
        pool.provision_private(_spec())

    assert provisioner.provision_count == 0
    assert not list((run_directory / "owned").glob("*.properties"))
    assert not list((root / "ccm-id-locks").glob("*.owner"))
    pool.close()
    assert not run_directory.exists()


def test_private_cluster_is_tracked_if_lease_creation_is_interrupted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool, provisioner = _pool(tmp_path)

    def interrupt_scope(_cluster: PhysicalTestCluster) -> TestResourceScope:
        raise KeyboardInterrupt

    monkeypatch.setattr(pool, "_create_resource_scope", interrupt_scope)

    with pytest.raises(KeyboardInterrupt):
        pool.provision_private(_spec())

    pool.close()
    assert provisioner.remove_count == 1


def test_private_retirement_clears_marker_if_success_transition_is_interrupted(
    tmp_path: Path,
) -> None:
    class InterruptingCondition:
        def __init__(self, condition: Any) -> None:  # noqa: ANN401 -- wraps private condition for fault injection
            self._condition = condition
            self._enters = 0

        def __enter__(self) -> object:
            self._enters += 1
            if self._enters == 2:
                raise KeyboardInterrupt
            return self._condition.__enter__()

        def __exit__(self, *args: object) -> object:
            return self._condition.__exit__(*args)

        def wait(self) -> bool:
            return cast(bool, self._condition.wait())

        def notify_all(self) -> None:
            self._condition.notify_all()

    pool, provisioner = _pool(tmp_path)
    lease = pool.provision_private(_spec())
    condition = InterruptingCondition(
        pool._condition  # noqa: SLF001 -- interrupt state-transition boundary
    )
    pool._condition = cast(Any, condition)  # noqa: SLF001 -- fault injection

    with pytest.raises(KeyboardInterrupt):
        lease.close()

    current = pool._current  # noqa: SLF001 -- assert retry state
    assert current is not None
    assert not current.retiring
    lease.close()
    assert provisioner.remove_count == 1
    pool.close()


def test_reusable_release_is_idempotent_if_interrupted_after_reference_removal(
    tmp_path: Path,
) -> None:
    class InterruptingSet(set[TestResourceScope]):
        def discard(self, element: object) -> None:
            super().discard(element)
            raise KeyboardInterrupt

    pool, provisioner = _pool(tmp_path)
    lease = pool.acquire_reusable(_spec())
    current = pool._current  # noqa: SLF001 -- fault-injection seam
    assert current is not None
    current.active_resources = InterruptingSet(current.active_resources)

    with pytest.raises(KeyboardInterrupt):
        lease.close()

    current.active_resources = set(current.active_resources)
    lease.close()
    lease.close()
    pool.close()
    assert provisioner.remove_count == 1


def test_interrupted_failed_provision_retains_durable_ownership(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    state = CcmRunState.open(root, lambda _run, _manifest: None)
    provisioner = FakeProvisioner(state.run_directory)
    provisioner.fail_provision_ambiguously = True
    pool = TestClusterPool(
        cast(CcmProvisioner, cast(Any, provisioner)),
        maximum_nodes=1,
        cleanup_resources=lambda _resources: None,
        run_state=state,
    )

    with pytest.raises(CcmClusterProvisioningError):
        pool.provision_private(_spec())

    assert list((state.run_directory / "owned").glob("*.properties"))
    assert list((root / "ccm-id-locks").glob("*.owner"))
    provisioner.fail_provision_ambiguously = False
    pool.close()
    assert not list((root / "ccm-id-locks").glob("*.owner"))


@pytest.mark.parametrize("value", ["0", "-1", "+1", " 1", "1 ", "one", "١"])
def test_positive_integer_override_is_strict(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("SCYLLA_CCM_MAX_NODES", value)
    with pytest.raises(RuntimeError, match="positive integer"):
        _parse_positive_integer("SCYLLA_CCM_MAX_NODES", 9)
