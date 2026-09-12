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

"""Native Scylla CCM provisioner for integration tests."""

from __future__ import annotations

import contextlib
import math
import os
import platform
import shutil
import signal
import ssl
import stat
import subprocess
import sys
import tempfile
import time
import urllib.request
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from alternator import Auth
from tests.testinfra.cluster import (
    PhysicalTestCluster,
    TestClusterNode,
    add_no_proxy_hosts,
)
from tests.testinfra.cluster_spec import (
    AlternatorTransport,
    ClusterSpec,
    parse_yaml_value,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence


PINNED_CCM_COMMIT = "d15a2fab9d22fffad8a30c806a7c8e1632e58aae"
HTTP_PORT = 8080
HTTPS_PORT = 8043
STORAGE_PORT = 7000
API_PORT = 10000
JMX_PORT = 7199
MAXIMUM_NODE_COUNT = 9

GOSSIPING_PROPERTY_FILE_SNITCH = (
    "org.apache.cassandra.locator.GossipingPropertyFileSnitch"
)
TEST_USER = "alternator_tests"
TEST_SALTED_PASSWORD = (
    "$6$IcPWfCigHWVhHTf.$h3.30m5R2CnYqIeniCumbXCBxBxvtYPP3MbZVsjKcu268ESO"
    "crUtSJwf1iO1s83KUT3waITRtTiexBdSWEI0Q/"
)
READINESS_TIMEOUT = 300.0
COMMAND_TIMEOUT = 600.0
PROCESS_TERMINATION_GRACE = 2.0
PROCESS_KILL_TIMEOUT = 5.0
CCM_IMPLICIT_NODE_KEYS = frozenset(
    {
        "hinted_handoff_enabled",
        "commitlog_sync",
        "commitlog_sync_period_in_ms",
        "commitlog_sync_batch_window_in_ms",
    }
)
UNSUPPORTED_CCM_ENVIRONMENT = frozenset(
    {"SCYLLA_EXT_ENV", "SCYLLA_EXT_OPTS", "SCYLLA_MANAGER_PACKAGE"}
)
_CCM_ARCHITECTURES = {
    "aarch64": "aarch64",
    "amd64": "x86_64",
    "arm64": "aarch64",
    "x86_64": "x86_64",
}


class _ProcessReferenceState(Enum):
    ABSENT = auto()
    DEAD = auto()
    OWNED = auto()


class _ProcessReferenceKind(Enum):
    SCYLLA = "Scylla"
    JMX = "JMX"
    AGENT = "manager agent"


class _PathState(Enum):
    ABSENT = auto()
    PRESENT = auto()
    UNKNOWN = auto()


class CcmCommandError(OSError):
    """CCM command returned nonzero or timed out after its group was reaped."""

    def __init__(
        self,
        command: Sequence[str],
        exit_code: int,
        output: str,
        *,
        timed_out: bool = False,
    ) -> None:
        self.command = tuple(command)
        self.exit_code = exit_code
        self.output = output
        self.timed_out = timed_out
        outcome = "timed out" if timed_out else f"failed with exit code {exit_code}"
        super().__init__(f"CCM command {outcome}: {' '.join(command)}\n{output}")


class CcmProcessCleanupError(OSError):
    """Harness cannot prove command-process cleanup; state must survive restart."""

    recovery_required = True


class CcmClusterProvisioningError(RuntimeError):
    """Initial provisioning failed and whole-cluster rollback did not complete."""

    def __init__(
        self,
        cluster: PhysicalTestCluster,
        provisioning_error: BaseException,
        rollback_error: BaseException,
    ) -> None:
        self.cluster = cluster
        self.provisioning_error = provisioning_error
        self.rollback_error = rollback_error
        self.recovery_required = requires_next_run_recovery(
            provisioning_error
        ) or requires_next_run_recovery(rollback_error)
        super().__init__(
            f"Failed to provision {cluster.instance_id!r} and CCM rollback failed"
        )
        self.__cause__ = provisioning_error


class CcmNodeProvisioningError(RuntimeError):
    """Node provisioning failed, with explicit rollback outcome."""

    def __init__(
        self,
        node: TestClusterNode,
        node_remains_provisioned: bool,
        cluster_state_ambiguous: bool,
        provisioning_error: BaseException,
        rollback_error: BaseException | None = None,
    ) -> None:
        self.node = node
        self.node_remains_provisioned = node_remains_provisioned
        self.cluster_state_ambiguous = cluster_state_ambiguous
        self.provisioning_error = provisioning_error
        self.rollback_error = rollback_error
        self.recovery_required = requires_next_run_recovery(
            provisioning_error
        ) or requires_next_run_recovery(rollback_error)
        if rollback_error is not None:
            message = f"Failed to provision {node.name!r} and CCM rollback failed"
        elif cluster_state_ambiguous:
            message = (
                f"Failed to provision {node.name!r}; local rollback succeeded but "
                "cluster membership is ambiguous"
            )
        else:
            message = f"Failed to provision {node.name!r}; CCM rollback succeeded"
        super().__init__(message)
        self.__cause__ = provisioning_error


def requires_next_run_recovery(failure: BaseException | None) -> bool:
    """Return whether failure chain reports unproven child-process cleanup."""
    pending = [failure]
    visited: set[int] = set()
    while pending:
        current = pending.pop()
        if current is None or id(current) in visited:
            continue
        visited.add(id(current))
        if isinstance(current, CcmProcessCleanupError) or bool(
            getattr(current, "recovery_required", False)
        ):
            return True
        pending.extend((current.__cause__, current.__context__))
        nested = getattr(current, "exceptions", ())
        if isinstance(nested, tuple):
            pending.extend(nested)
        rollback = getattr(current, "rollback_error", None)
        if isinstance(rollback, BaseException):
            pending.append(rollback)
    return False


class CcmProvisioner:
    """Translate immutable cluster specifications into native CCM clusters."""

    def __init__(
        self,
        run_directory: Path,
        diagnostics_directory: Path | None = None,
        ccm_executable: str | None = None,
        command_timeout: float = COMMAND_TIMEOUT,
        readiness_timeout: float = READINESS_TIMEOUT,
    ) -> None:
        operational = diagnostics_directory is None and ccm_executable is None
        requested_run = run_directory.absolute()
        requested_run.mkdir(parents=True, exist_ok=True)
        self._reject_unsafe_directory(requested_run, "run")
        self._run_directory = requested_run.resolve(strict=True)
        self._clusters_directory = self._run_directory / "clusters"
        self._clusters_directory.mkdir(exist_ok=True)
        self._validate_direct_child(
            self._run_directory, self._clusters_directory, "clusters"
        )

        configured_diagnostics = os.environ.get("SCYLLA_CCM_DIAGNOSTICS_DIR")
        if diagnostics_directory is None:
            diagnostics_directory = Path(configured_diagnostics or "target/ccm")
        diagnostics_directory = diagnostics_directory.absolute()
        diagnostics_directory.mkdir(parents=True, exist_ok=True)
        self._reject_unsafe_directory(diagnostics_directory, "diagnostics")
        self._diagnostics_directory = diagnostics_directory.resolve(strict=True)
        if operational:
            self.validate_operational_directories(
                self._run_directory, self._diagnostics_directory
            )

        if command_timeout <= 0 or readiness_timeout <= 0:
            raise ValueError("CCM command and readiness timeouts must be positive")
        configured_ccm = ccm_executable or os.environ.get("SCYLLA_CCM_PATH") or "ccm"
        resolved_ccm = shutil.which(configured_ccm)
        if resolved_ccm is not None:
            configured_ccm = str(Path(resolved_ccm).resolve(strict=True))
        elif os.sep in configured_ccm:
            configured_ccm = os.path.abspath(configured_ccm)
        self._ccm_executable = configured_ccm
        self._command_timeout = command_timeout
        self._readiness_timeout = readiness_timeout

    @property
    def run_directory(self) -> Path:
        return self._run_directory

    @property
    def diagnostics_directory(self) -> Path:
        return self._diagnostics_directory

    @staticmethod
    def validate_operational_directories(
        run_directory: Path, diagnostics_directory: Path
    ) -> None:
        normalized_run = run_directory.absolute()
        runs_directory = normalized_run.parent
        state_root = runs_directory.parent
        if runs_directory.name != "runs" or state_root == runs_directory:
            raise OSError(
                "Operational CCM run directory is not beneath state-root runs directory"
            )
        normalized_diagnostics = diagnostics_directory.absolute()
        if CcmProvisioner._is_relative_to(
            normalized_diagnostics, state_root
        ) or CcmProvisioner._is_relative_to(state_root, normalized_diagnostics):
            raise OSError(
                f"CCM diagnostics directory must not overlap harness state root {state_root}"
            )

    def requires_jmx_port_reservation(self, spec: ClusterSpec) -> bool:
        default_version = getattr(
            ClusterSpec, "DEFAULT_SCYLLA_VERSION", "release:2025.2.5"
        )
        return (
            spec.scylla_version != default_version
            or not self.is_repository_pinned_ccm(self._ccm_executable, Path.cwd())
        )

    @staticmethod
    def is_repository_pinned_ccm(executable: str, project_directory: Path) -> bool:
        try:
            environment = (
                project_directory.absolute() / f"bin/scylla-ccm-{PINNED_CCM_COMMIT}"
            )
            expected = environment / "bin/ccm"
            marker = environment / ".install-complete"
            candidate = Path(executable).absolute()
            return (
                candidate.is_file()
                and os.access(candidate, os.X_OK)
                and not candidate.is_symlink()
                and marker.is_file()
                and not marker.is_symlink()
                and candidate.resolve(strict=True) == expected.resolve(strict=True)
                and marker.read_text(encoding="ascii").strip() == PINNED_CCM_COMMIT
            )
        except (OSError, RuntimeError):
            return False

    def provision(
        self,
        spec: ClusterSpec,
        instance_id: str,
        ccm_id: int,
        *,
        on_provisioned: Callable[[PhysicalTestCluster], None] | None = None,
    ) -> PhysicalTestCluster:
        if not 1 <= ccm_id <= 99:
            raise ValueError(f"Invalid CCM ID {ccm_id}")
        ccm_directory = self._owned_child(self._clusters_directory, instance_id)
        ccm_directory.mkdir(exist_ok=True)
        self._validate_direct_child(
            self._clusters_directory, ccm_directory, "CCM config"
        )
        nodes = self._build_nodes(spec, ccm_id)
        add_no_proxy_hosts(node.address for node in nodes)
        ca_certificate_path: Path | None = None
        credentials: Auth | None = None
        try:
            if AlternatorTransport.HTTPS in spec.transports:
                ca_certificate_path = self._create_certificate_authority(
                    instance_id, ccm_directory
                )
            if spec.security.enforce_alternator_authorization:
                credentials = Auth.static_credentials(TEST_USER, TEST_SALTED_PASSWORD)

            arguments = [
                "create",
                "--config-dir",
                str(ccm_directory),
                instance_id,
                "--scylla",
                "--version",
                spec.scylla_version,
                "--nodes",
                self._first_rack_counts(spec),
                "--id",
                str(ccm_id),
            ]
            if len(spec.topology.datacenters) > 1:
                arguments.extend(["--snitch", GOSSIPING_PROPERTY_FILE_SNITCH])
            self._run_ccm(ccm_directory, arguments)
            self._add_additional_rack_nodes(spec, nodes, ccm_directory)
            self._configure(spec, nodes, ccm_directory)
            if ca_certificate_path is not None:
                for node in nodes:
                    self._configure_node_certificate(
                        node, ccm_directory, ca_certificate_path
                    )
            self.apply_yaml_overrides(
                spec, ccm_directory, nodes, update_cluster_state=False
            )
            self.verify_yaml_overrides(spec, ccm_directory, nodes)

            cluster = PhysicalTestCluster(
                self,
                instance_id,
                ccm_id,
                ccm_directory,
                spec,
                nodes,
                ca_certificate_path,
                credentials,
            )
            self.start(cluster)
            if on_provisioned is not None:
                on_provisioned(cluster)
            return cluster
        except BaseException as provisioning_error:
            diagnostic_error: BaseException | None = None
            try:
                self.collect_diagnostics(instance_id, ccm_directory)
            except BaseException as exception:
                diagnostic_error = exception
            retained_error = diagnostic_error
            if isinstance(provisioning_error, CcmProcessCleanupError):
                retained_error = CcmProcessCleanupError(
                    "CCM command processes remain owned by failed cluster"
                )
            if retained_error is None:
                try:
                    self._remove_by_name(instance_id, ccm_directory)
                except BaseException as exception:
                    retained_error = exception
            if retained_error is not None:
                failed_cluster = PhysicalTestCluster(
                    self,
                    instance_id,
                    ccm_id,
                    ccm_directory,
                    spec,
                    nodes,
                    ca_certificate_path,
                    credentials,
                )
                raise CcmClusterProvisioningError(
                    failed_cluster, provisioning_error, retained_error
                ) from provisioning_error
            raise

    def start(
        self,
        cluster: PhysicalTestCluster,
        nodes: Sequence[TestClusterNode] | None = None,
    ) -> None:
        selected = tuple(nodes if nodes is not None else cluster.nodes)
        for node in selected:
            if self._prepare_node_for_start(cluster, node):
                self._run_ccm(
                    cluster.ccm_directory, self._start_arguments(cluster, node.name)
                )
        self._wait_for_alternator(cluster, selected, self._readiness_timeout)

    def stop(self, cluster: PhysicalTestCluster) -> None:
        cluster_directory = self._current_cluster_directory(cluster.ccm_directory)
        if cluster_directory is None:
            raise OSError(f"CCM has no current cluster under {cluster.ccm_directory}")
        if not self._prepare_cluster_process_references(cluster_directory):
            return
        self._run_stop_and_verify(
            cluster.ccm_directory,
            cluster_directory,
            None,
            ["stop", "--config-dir", str(cluster.ccm_directory)],
        )

    def start_node(self, cluster: PhysicalTestCluster, node: TestClusterNode) -> None:
        if self._prepare_node_for_start(cluster, node):
            self._run_ccm(
                cluster.ccm_directory, self._start_arguments(cluster, node.name)
            )
        self.wait_for_node_ready(cluster, node)

    def stop_node(self, cluster: PhysicalTestCluster, node: TestClusterNode) -> None:
        cluster_directory = self._current_cluster_directory(cluster.ccm_directory)
        if cluster_directory is None:
            raise OSError(f"CCM has no current cluster under {cluster.ccm_directory}")
        if not self._prepare_node_process_references(cluster_directory, node.name):
            return
        self._run_stop_and_verify(
            cluster.ccm_directory,
            cluster_directory,
            node.name,
            [node.name, "stop", "--config-dir", str(cluster.ccm_directory)],
        )

    def _prepare_node_for_start(
        self, cluster: PhysicalTestCluster, node: TestClusterNode
    ) -> bool:
        if self.is_node_running(cluster, node):
            return False
        cluster_directory = self._current_cluster_directory(cluster.ccm_directory)
        if cluster_directory is None:
            raise OSError(f"CCM has no current cluster under {cluster.ccm_directory}")
        if self._prepare_node_process_references(cluster_directory, node.name):
            raise CcmProcessCleanupError(
                f"Cannot restart CCM node {node.name!r} while an ancillary "
                "node process remains alive"
            )
        return True

    def _run_stop_and_verify(
        self,
        ccm_directory: Path,
        cluster_directory: Path,
        node_name: str | None,
        arguments: Sequence[str],
    ) -> None:
        command_error: BaseException | None = None
        try:
            self._run_ccm(ccm_directory, arguments)
        except BaseException as exception:
            command_error = exception
        try:
            self._await_stopped_process_references(cluster_directory, node_name)
        except BaseException as verification_error:
            if command_error is not None:
                verification_error.__context__ = command_error
            raise
        if command_error is not None and not isinstance(command_error, CcmCommandError):
            raise command_error

    def _await_stopped_process_references(
        self, cluster_directory: Path, node_name: str | None
    ) -> None:
        deadline = time.monotonic() + PROCESS_TERMINATION_GRACE
        while True:
            try:
                remains = (
                    self._prepare_cluster_process_references(cluster_directory)
                    if node_name is None
                    else self._prepare_node_process_references(
                        cluster_directory, node_name
                    )
                )
            except CcmProcessCleanupError:
                raise
            except BaseException as exception:
                raise CcmProcessCleanupError(
                    "Cannot verify process cleanup after CCM stop"
                ) from exception
            if not remains:
                return
            if time.monotonic() >= deadline:
                description = "cluster" if node_name is None else f"node {node_name!r}"
                raise CcmProcessCleanupError(
                    f"CCM {description} stop left owned processes alive"
                )
            time.sleep(0.02)

    def add_node(
        self, cluster: PhysicalTestCluster, datacenter: str, rack: str
    ) -> TestClusterNode:
        if len(cluster.nodes) >= MAXIMUM_NODE_COUNT:
            raise RuntimeError(f"Cluster cannot exceed {MAXIMUM_NODE_COUNT} nodes")
        index = 1
        existing = {node.name for node in cluster.nodes}
        while f"node{index}" in existing:
            index += 1
        node = TestClusterNode(
            name=f"node{index}",
            address=f"127.0.{cluster.ccm_id}.{index}",
            datacenter=datacenter,
            rack=rack,
        )
        add_no_proxy_hosts((node.address,))
        start_attempted = False
        try:
            self._run_ccm(
                cluster.ccm_directory,
                [
                    "add",
                    "--config-dir",
                    str(cluster.ccm_directory),
                    node.name,
                    "--scylla",
                    "--seeds",
                    "--auto-bootstrap",
                    "--itf",
                    node.address,
                    "--data-center",
                    datacenter,
                    "--rack",
                    rack,
                ],
            )
            if cluster.ca_certificate_path is not None:
                self._configure_node_certificate(
                    node, cluster.ccm_directory, cluster.ca_certificate_path
                )
            self.apply_yaml_overrides(
                cluster.spec,
                cluster.ccm_directory,
                [node],
                update_cluster_state=False,
            )
            self.verify_yaml_overrides(cluster.spec, cluster.ccm_directory, [node])
            start_attempted = True
            self.start_node(cluster, node)
            return node
        except BaseException as provisioning_error:
            diagnostic_error: BaseException | None = None
            try:
                self.collect_diagnostics(cluster.instance_id, cluster.ccm_directory)
            except BaseException as exception:
                diagnostic_error = exception
            if isinstance(provisioning_error, CcmProcessCleanupError):
                retained = CcmProcessCleanupError(
                    "CCM command processes remain owned by failed node"
                )
                raise CcmNodeProvisioningError(
                    node, True, True, provisioning_error, retained
                ) from provisioning_error
            if diagnostic_error is not None:
                raise CcmNodeProvisioningError(
                    node, True, True, provisioning_error, diagnostic_error
                ) from provisioning_error
            try:
                self._remove_node_by_name(cluster.ccm_directory, node.name)
            except BaseException as rollback_error:
                raise CcmNodeProvisioningError(
                    node, True, True, provisioning_error, rollback_error
                ) from provisioning_error
            raise CcmNodeProvisioningError(
                node,
                False,
                start_attempted,
                provisioning_error,
            ) from provisioning_error

    def decommission_node(
        self, cluster: PhysicalTestCluster, node: TestClusterNode
    ) -> None:
        self._run_ccm(
            cluster.ccm_directory,
            [
                node.name,
                "decommission",
                "--config-dir",
                str(cluster.ccm_directory),
            ],
        )

    def delete_node_state(
        self, cluster: PhysicalTestCluster, node: TestClusterNode
    ) -> None:
        self.collect_diagnostics(cluster.instance_id, cluster.ccm_directory)
        self._remove_node_by_name(cluster.ccm_directory, node.name)

    def is_healthy(self, cluster: PhysicalTestCluster) -> bool:
        try:
            self._wait_for_alternator(cluster, cluster.nodes, 10.0)
            return True
        except Exception:
            return False

    def is_node_running(
        self, cluster: PhysicalTestCluster, node: TestClusterNode
    ) -> bool:
        cluster_directory = self._current_cluster_directory(cluster.ccm_directory)
        if cluster_directory is None:
            return False
        node_directory = self._owned_child(cluster_directory, node.name)
        if not node_directory.exists():
            return False
        self._validate_direct_child(cluster_directory, node_directory, "node")
        node_config = node_directory / "node.conf"
        configured_pid: int | None = None
        configured_state = _ProcessReferenceState.ABSENT
        if node_config.exists():
            config = self._read_yaml_map(node_config)
            self._validate_native_node_metadata(config, node_directory)
            if "pid" in config:
                configured_pid = self._parse_pid_reference(config["pid"], node_config)
                configured_state = self._inspect_process_reference(
                    configured_pid,
                    node_config,
                    node_directory,
                    _ProcessReferenceKind.SCYLLA,
                )
        pid_file = node_directory / "cassandra.pid"
        pid = self._read_pid_reference(pid_file)
        pid_state = self._inspect_process_reference(
            pid, pid_file, node_directory, _ProcessReferenceKind.SCYLLA
        )
        if pid_state is _ProcessReferenceState.OWNED and (
            configured_state is not _ProcessReferenceState.OWNED
            or pid != configured_pid
        ):
            raise CcmProcessCleanupError(
                f"CCM cannot trust Scylla PID {pid}; node.conf does not "
                "reference same owned process"
            )
        return configured_state is _ProcessReferenceState.OWNED

    def wait_for_node_ready(
        self, cluster: PhysicalTestCluster, node: TestClusterNode
    ) -> None:
        self._wait_for_alternator(cluster, [node], self._readiness_timeout)

    def remove(self, cluster: PhysicalTestCluster) -> None:
        self.collect_diagnostics(cluster.instance_id, cluster.ccm_directory)
        self._remove_by_name(cluster.instance_id, cluster.ccm_directory)

    def cleanup_stale_cluster(
        self, instance_id: str, ccm_id: int, ccm_directory: Path
    ) -> None:
        if not 1 <= ccm_id <= 99:
            raise OSError(f"Invalid CCM ID {ccm_id}")
        normalized = self._validate_ccm_directory_location(
            ccm_directory, require_exists=False
        )
        state = self._path_state(normalized)
        if state is _PathState.ABSENT:
            return
        if state is _PathState.UNKNOWN:
            raise OSError(f"Cannot determine stale CCM config state at {normalized}")
        self.collect_diagnostics(instance_id, normalized)
        cluster_directory = self._owned_child(normalized, instance_id)
        cluster_config_state = self._path_state(cluster_directory / "cluster.conf")
        if cluster_config_state is _PathState.ABSENT:
            self._cleanup_absent_cluster_state(instance_id, normalized)
            return
        if cluster_config_state is _PathState.UNKNOWN:
            raise OSError(
                f"Cannot determine stale CCM cluster state for {instance_id!r}"
            )
        self._validate_cluster_metadata_if_present(cluster_directory, instance_id)
        self._sanitize_stale_process_references(cluster_directory)
        self._remove_by_name(instance_id, normalized)

    def apply_yaml_overrides(
        self,
        spec: ClusterSpec,
        ccm_directory: Path,
        nodes: Sequence[TestClusterNode],
        *,
        update_cluster_state: bool,
    ) -> None:
        if not spec.scylla_yaml_overrides:
            return
        cluster_directory = self._current_cluster_directory(ccm_directory)
        if cluster_directory is None:
            raise OSError(f"CCM has no current cluster under {ccm_directory}")
        overrides = self._parse_yaml_overrides(spec.scylla_yaml_overrides)
        if update_cluster_state:
            cluster_config = cluster_directory / "cluster.conf"
            cluster_yaml = self._read_yaml_map(cluster_config)
            config_options = self._child_map(
                cluster_yaml, "config_options", create=True
            )
            if config_options is None:
                raise OSError("CCM cluster configuration has no config_options mapping")
            self._apply_dotted_values(config_options, overrides)
            self._write_yaml_map(cluster_config, cluster_yaml)

        overridden_implicit = CCM_IMPLICIT_NODE_KEYS.intersection(overrides)
        for node in nodes:
            node_directory = self._owned_child(cluster_directory, node.name)
            node_config = node_directory / "node.conf"
            if overridden_implicit and node_config.is_file():
                node_yaml = self._read_yaml_map(node_config)
                config_options = self._child_map(
                    node_yaml, "config_options", create=False
                )
                if config_options is not None:
                    for key in overridden_implicit:
                        config_options.pop(key, None)
                    self._write_yaml_map(node_config, node_yaml)
            scylla_config = node_directory / "conf/scylla.yaml"
            scylla_yaml = self._read_yaml_map(scylla_config)
            self._apply_dotted_values(scylla_yaml, overrides)
            self._write_yaml_map(scylla_config, scylla_yaml)

    def verify_yaml_overrides(
        self,
        spec: ClusterSpec,
        ccm_directory: Path,
        nodes: Sequence[TestClusterNode],
    ) -> None:
        if not spec.scylla_yaml_overrides:
            return
        cluster_directory = self._current_cluster_directory(ccm_directory)
        if cluster_directory is None:
            raise OSError(f"CCM has no current cluster under {ccm_directory}")
        expected = self._parse_yaml_overrides(spec.scylla_yaml_overrides)
        cluster_yaml = self._read_yaml_map(cluster_directory / "cluster.conf")
        config_options = self._child_map(cluster_yaml, "config_options", create=False)
        self._verify_dotted_values(
            config_options,
            expected,
            f"CCM cluster configuration {cluster_directory / 'cluster.conf'}",
        )
        for node in nodes:
            scylla_config = (
                self._owned_child(cluster_directory, node.name) / "conf/scylla.yaml"
            )
            self._verify_dotted_values(
                self._read_yaml_map(scylla_config),
                expected,
                f"Scylla configuration {scylla_config}",
            )

    def collect_diagnostics(self, instance_id: str, ccm_directory: Path) -> None:
        source = self._validate_ccm_directory_location(
            ccm_directory, require_exists=False
        )
        source_state = self._path_state(source)
        if source_state is _PathState.ABSENT:
            return
        if source_state is _PathState.UNKNOWN:
            raise OSError(f"Cannot determine diagnostic source state at {source}")
        destination = self._owned_child(self._diagnostics_directory, instance_id)
        self._prepare_directory(destination, self._diagnostics_directory, "diagnostic")
        for command_log in source.iterdir():
            if command_log.name == "ccm-commands.log" or (
                command_log.name.startswith("ccm-command-")
                and command_log.name.endswith(".log")
            ):
                self._copy_volatile_diagnostic(
                    command_log, destination, Path(command_log.name)
                )

        cluster_directory = self._owned_child(source, instance_id)
        cluster_state = self._path_state(cluster_directory)
        if cluster_state is _PathState.ABSENT:
            return
        if cluster_state is _PathState.UNKNOWN:
            raise OSError(
                f"Cannot determine diagnostic cluster state at {cluster_directory}"
            )
        self._validate_direct_child(source, cluster_directory, "cluster")
        self._copy_optional_diagnostic(
            cluster_directory / "cluster.conf",
            destination,
            Path(instance_id) / "cluster.conf",
        )
        for node_directory in cluster_directory.iterdir():
            if not self._is_ccm_node_name(node_directory.name):
                continue
            self._validate_direct_child(cluster_directory, node_directory, "node")
            node_relative = Path(instance_id) / node_directory.name
            self._copy_optional_diagnostic(
                node_directory / "node.conf",
                destination,
                node_relative / "node.conf",
            )
            configuration = self._owned_child(node_directory, "conf")
            configuration_state = self._path_state(configuration)
            if configuration_state is _PathState.PRESENT:
                self._validate_direct_child(
                    node_directory, configuration, "node configuration"
                )
                self._copy_optional_diagnostic(
                    configuration / "scylla.yaml",
                    destination,
                    node_relative / "conf/scylla.yaml",
                )
            elif configuration_state is _PathState.UNKNOWN:
                raise OSError(
                    f"Cannot determine CCM node configuration state at {configuration}"
                )
            logs = node_directory / "logs"
            logs_state = self._path_state(logs)
            if logs_state is _PathState.PRESENT:
                self._reject_unsafe_directory(logs, "node logs")
                for root, directories, files in os.walk(logs, followlinks=False):
                    root_path = Path(root)
                    for directory in directories:
                        if (root_path / directory).is_symlink():
                            raise OSError(
                                f"Refusing symbolic link in CCM logs: {root_path / directory}"
                            )
                    for filename in files:
                        path = root_path / filename
                        if path.is_symlink():
                            raise OSError(f"Refusing symbolic link in CCM logs: {path}")
                        if not self._is_private_key_file(path):
                            relative = node_relative / "logs" / path.relative_to(logs)
                            self._copy_volatile_diagnostic(path, destination, relative)
            elif logs_state is _PathState.UNKNOWN:
                raise OSError(f"Cannot determine CCM log state at {logs}")

    def _configure(
        self,
        spec: ClusterSpec,
        nodes: Sequence[TestClusterNode],
        ccm_directory: Path,
    ) -> None:
        options: dict[str, str] = {
            "alternator_write_isolation": "only_rmw_uses_lwt",
            "endpoint_snitch": GOSSIPING_PROPERTY_FILE_SNITCH,
            "start_native_transport": "true",
        }
        if AlternatorTransport.HTTP in spec.transports:
            options["alternator_port"] = str(HTTP_PORT)
        if AlternatorTransport.HTTPS in spec.transports:
            options["alternator_https_port"] = str(HTTPS_PORT)
            options["alternator_encryption_options.enable_session_tickets"] = "true"
        authentication = spec.security.authentication.name
        authorization = spec.security.authorization.name
        if authentication != "ALLOW_ALL":
            options["authenticator"] = self._authenticator(authentication)
            options["auth_superuser_name"] = TEST_USER
            options["auth_superuser_salted_password"] = TEST_SALTED_PASSWORD
        if authorization != "ALLOW_ALL":
            options["authorizer"] = self._authorizer(authorization)
        if spec.security.enforce_alternator_authorization:
            options["alternator_enforce_authorization"] = "true"
        options.update(spec.scylla_yaml_overrides)
        arguments = ["updateconf", "--config-dir", str(ccm_directory)]
        arguments.extend(f"{key}:{options[key]}" for key in sorted(options))
        self._run_ccm(ccm_directory, arguments)
        self.apply_yaml_overrides(spec, ccm_directory, nodes, update_cluster_state=True)

    def _create_certificate_authority(
        self, instance_id: str, ccm_directory: Path
    ) -> Path:
        tls_directory = ccm_directory / "tls"
        tls_directory.mkdir(exist_ok=True)
        certificate = tls_directory / "ca.crt"
        self._run_command(
            ccm_directory,
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:3072",
                "-sha256",
                "-nodes",
                "-days",
                "730",
                "-subj",
                f"/CN={instance_id} test CA",
                "-keyout",
                str(tls_directory / "ca.key"),
                "-out",
                str(certificate),
            ],
        )
        return certificate

    def _configure_node_certificate(
        self,
        node: TestClusterNode,
        ccm_directory: Path,
        ca_certificate_path: Path,
    ) -> None:
        tls_directory = ccm_directory / "tls"
        node_directory = tls_directory / node.name
        node_directory.mkdir(exist_ok=True)
        certificate = node_directory / "server.crt"
        key = node_directory / "server.key"
        request = node_directory / "server.csr"
        extension = node_directory / "server.ext"
        extension.write_text(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth\n"
            f"subjectAltName=IP:{node.address}\n",
            encoding="ascii",
        )
        self._run_command(
            ccm_directory,
            [
                "openssl",
                "req",
                "-newkey",
                "rsa:3072",
                "-sha256",
                "-nodes",
                "-subj",
                f"/CN={node.name}",
                "-keyout",
                str(key),
                "-out",
                str(request),
            ],
        )
        self._run_command(
            ccm_directory,
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                str(request),
                "-CA",
                str(ca_certificate_path),
                "-CAkey",
                str(tls_directory / "ca.key"),
                "-CAcreateserial",
                "-days",
                "365",
                "-sha256",
                "-extfile",
                str(extension),
                "-out",
                str(certificate),
            ],
        )
        self._run_ccm(
            ccm_directory,
            [
                node.name,
                "updateconf",
                "--config-dir",
                str(ccm_directory),
                f"alternator_encryption_options.certificate:{certificate}",
                f"alternator_encryption_options.keyfile:{key}",
            ],
        )

    def _wait_for_alternator(
        self,
        cluster: PhysicalTestCluster,
        nodes: Sequence[TestClusterNode],
        timeout: float,
    ) -> None:
        deadline = time.monotonic() + timeout
        endpoints: list[str] = []
        for node in nodes:
            if AlternatorTransport.HTTP in cluster.spec.transports:
                endpoints.append(f"http://{node.address}:{HTTP_PORT}/")
            if AlternatorTransport.HTTPS in cluster.spec.transports:
                endpoints.append(f"https://{node.address}:{HTTPS_PORT}/")
        last_exception: BaseException | None = None
        while time.monotonic() < deadline:
            all_ready = True
            for endpoint in endpoints:
                try:
                    request = urllib.request.Request(endpoint, method="GET")
                    context = (
                        ssl._create_unverified_context()  # noqa: SLF001 -- readiness only
                        if endpoint.startswith("https://")
                        else None
                    )
                    handlers: list[urllib.request.BaseHandler] = [
                        urllib.request.ProxyHandler({})
                    ]
                    if context is not None:
                        handlers.append(urllib.request.HTTPSHandler(context=context))
                    opener = urllib.request.build_opener(*handlers)
                    with opener.open(request, timeout=5.0) as response:  # noqa: S310 -- loopback CCM endpoint
                        if not 200 <= response.status < 300:
                            all_ready = False
                            break
                except Exception as exception:
                    last_exception = exception
                    all_ready = False
                    break
            if all_ready:
                return
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
        raise TimeoutError(
            f"Alternator endpoints for cluster {cluster.instance_id!r} did not become ready"
        ) from last_exception

    def _run_ccm(self, ccm_directory: Path, arguments: Sequence[str]) -> None:
        if arguments and arguments[0] != "create":
            current = self._current_cluster_directory(ccm_directory)
            if current is not None:
                self._validate_cluster_metadata_if_present(current, current.name)
        self._run_command(ccm_directory, [self._ccm_executable, *arguments])

    def _run_command(self, ccm_directory: Path, command: Sequence[str]) -> None:
        if not sys.platform.startswith("linux"):
            raise NotImplementedError(
                "Native scylla-ccm harness currently supports Linux only"
            )
        owned_directory = self._validate_ccm_directory_location(
            ccm_directory, require_exists=True
        )
        environment = self.configure_ccm_environment(os.environ, self._run_directory)
        descriptor, output_name = tempfile.mkstemp(
            prefix="ccm-command-", suffix=".log", dir=owned_directory
        )
        output_path = Path(output_name)
        command_text = " ".join(command)
        process: subprocess.Popen[bytes] | None = None
        timed_out = False
        exit_code = -1
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as output:
                output.write(f"> {command_text}\n".encode())
                output.flush()
                os.fsync(output.fileno())
                try:
                    process = subprocess.Popen(
                        list(command),
                        cwd=owned_directory,
                        env=environment,
                        stdout=output,
                        stderr=subprocess.STDOUT,
                        shell=False,
                        start_new_session=True,
                    )
                except OSError:
                    output.write(b"[exit start failed]\n")
                    raise
                try:
                    exit_code = process.wait(timeout=self._command_timeout)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    self._terminate_process_group(process)
                else:
                    if exit_code != 0:
                        self._terminate_process_group(process)
                outcome = "timeout" if timed_out else str(exit_code)
                output.write(f"[exit {outcome}]\n".encode())
                output.flush()
                os.fsync(output.fileno())
        except BaseException as primary_error:
            cleanup_error: BaseException | None = None
            if process is not None and process.poll() is None:
                try:
                    self._terminate_process_group(process)
                except BaseException as exception:
                    cleanup_error = exception
            try:
                self._append_aggregate_log(owned_directory, output_path)
            except BaseException as logging_error:
                if cleanup_error is not None:
                    cleanup_error.__context__ = logging_error
                else:
                    primary_error.__context__ = logging_error
            if cleanup_error is not None:
                cleanup_error.__context__ = primary_error
                raise cleanup_error from primary_error
            raise
        self._append_aggregate_log(owned_directory, output_path)
        command_output = output_path.read_text(encoding="utf-8", errors="replace")
        if timed_out or exit_code != 0:
            raise CcmCommandError(
                command, exit_code, command_output, timed_out=timed_out
            )

    def _terminate_process_group(self, process: subprocess.Popen[bytes]) -> None:
        process_group = process.pid
        try:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process_group, signal.SIGTERM)
            self._wait_for_process_group(process_group, PROCESS_TERMINATION_GRACE)
            if self._process_group_has_live_members(process_group):
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process_group, signal.SIGKILL)
                self._wait_for_process_group(process_group, PROCESS_KILL_TIMEOUT)
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=PROCESS_KILL_TIMEOUT)
            if (
                self._process_group_has_live_members(process_group)
                or process.poll() is None
            ):
                raise CcmProcessCleanupError(
                    f"CCM command process group {process_group} survived termination"
                )
        except CcmProcessCleanupError:
            raise
        except BaseException as exception:
            raise CcmProcessCleanupError(
                f"Unable to prove cleanup of CCM command process group {process_group}"
            ) from exception

    @staticmethod
    def _process_group_has_live_members(process_group: int) -> bool:
        for process_directory in Path("/proc").iterdir():
            if (
                not process_directory.name.isascii()
                or not process_directory.name.isdigit()
            ):
                continue
            try:
                value = (process_directory / "stat").read_text(encoding="ascii")
            except FileNotFoundError:
                continue
            command_end = value.rfind(")")
            if command_end < 0 or command_end + 2 >= len(value):
                raise OSError(
                    f"Unable to parse process state at {process_directory / 'stat'}"
                )
            fields = value[command_end + 2 :].split()
            if len(fields) < 3:
                raise OSError(
                    f"Unable to parse process state at {process_directory / 'stat'}"
                )
            try:
                candidate_group = int(fields[2])
            except ValueError as exception:
                raise OSError(
                    f"Unable to parse process group at {process_directory / 'stat'}"
                ) from exception
            if candidate_group == process_group and fields[0] != "Z":
                return True
        return False

    @staticmethod
    def _wait_for_process_group(process_group: int, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while (
            CcmProvisioner._process_group_has_live_members(process_group)
            and time.monotonic() < deadline
        ):
            time.sleep(0.02)

    @staticmethod
    def configure_ccm_environment(
        source: Mapping[str, str], run_directory: Path
    ) -> dict[str, str]:
        environment = dict(source)
        for variable in UNSUPPORTED_CCM_ENVIRONMENT:
            environment.pop(variable, None)
        environment["SCYLLA_CCM_RUN_DIR"] = str(run_directory)
        # Pinned CCM otherwise defaults every release download to x86_64.
        machine = platform.machine().strip().lower()
        try:
            environment["SCYLLA_ARCH"] = _CCM_ARCHITECTURES[machine]
        except KeyError as exception:
            raise OSError(
                f"Native scylla-ccm harness does not support architecture {machine!r}"
            ) from exception
        return environment

    @staticmethod
    def _append_aggregate_log(ccm_directory: Path, output_path: Path) -> None:
        if not output_path.exists():
            return
        with (ccm_directory / "ccm-commands.log").open("ab") as aggregate:
            aggregate.write(output_path.read_bytes())
            aggregate.flush()
            os.fsync(aggregate.fileno())

    @staticmethod
    def _start_arguments(
        cluster: PhysicalTestCluster, node_name: str | None
    ) -> list[str]:
        arguments: list[str] = []
        if node_name is not None:
            arguments.append(node_name)
        arguments.extend(
            [
                "start",
                "--config-dir",
                str(cluster.ccm_directory),
                "--wait-for-binary-proto",
                "--wait-other-notice",
                "--jvm_arg=--smp",
                f"--jvm_arg={cluster.spec.resources.smp}",
                "--jvm_arg=--memory",
                f"--jvm_arg={cluster.spec.resources.memory_mib}M",
            ]
        )
        return arguments

    @staticmethod
    def _build_nodes(spec: ClusterSpec, ccm_id: int) -> list[TestClusterNode]:
        nodes: list[TestClusterNode] = []
        index = 0
        for datacenter_index, datacenter in enumerate(spec.topology.datacenters, 1):
            for _ in range(datacenter.racks[0].node_count):
                index += 1
                nodes.append(
                    TestClusterNode(
                        f"node{index}",
                        f"127.0.{ccm_id}.{index}",
                        f"dc{datacenter_index}",
                        "RAC1",
                    )
                )
        for datacenter_index, datacenter in enumerate(spec.topology.datacenters, 1):
            for rack_index, rack in enumerate(datacenter.racks[1:], 2):
                for _ in range(rack.node_count):
                    index += 1
                    nodes.append(
                        TestClusterNode(
                            f"node{index}",
                            f"127.0.{ccm_id}.{index}",
                            f"dc{datacenter_index}",
                            f"RAC{rack_index}",
                        )
                    )
        return nodes

    @staticmethod
    def _first_rack_counts(spec: ClusterSpec) -> str:
        return ":".join(
            str(datacenter.racks[0].node_count)
            for datacenter in spec.topology.datacenters
        )

    def _add_additional_rack_nodes(
        self,
        spec: ClusterSpec,
        nodes: Sequence[TestClusterNode],
        ccm_directory: Path,
    ) -> None:
        first_rack_node_count = sum(
            datacenter.racks[0].node_count for datacenter in spec.topology.datacenters
        )
        for node in nodes[first_rack_node_count:]:
            # Initial topology is assembled before any node starts, so these nodes
            # must not bootstrap against a live ring. Dynamic add_node() does.
            self._run_ccm(
                ccm_directory,
                [
                    "add",
                    "--config-dir",
                    str(ccm_directory),
                    node.name,
                    "--scylla",
                    "--seeds",
                    "--itf",
                    node.address,
                    "--data-center",
                    node.datacenter,
                    "--rack",
                    node.rack,
                ],
            )

    @staticmethod
    def _authenticator(mode: str) -> str:
        if mode == "PASSWORD":
            return "org.apache.cassandra.auth.PasswordAuthenticator"
        if mode == "TRANSITIONAL":
            return "com.scylladb.auth.TransitionalAuthenticator"
        raise ValueError(f"No authenticator for {mode}")

    @staticmethod
    def _authorizer(mode: str) -> str:
        if mode == "CASSANDRA":
            return "org.apache.cassandra.auth.CassandraAuthorizer"
        if mode == "TRANSITIONAL":
            return "com.scylladb.auth.TransitionalAuthorizer"
        raise ValueError(f"No authorizer for {mode}")

    @staticmethod
    def _parse_yaml_overrides(overrides: Mapping[str, str]) -> dict[str, object]:
        parsed = {key: parse_yaml_value(value) for key, value in overrides.items()}
        for key, value in tuple(parsed.items()):
            root, separator, child = key.partition(".")
            if not separator or root not in parsed:
                continue
            parent = parsed[root]
            if not isinstance(parent, dict):
                raise OSError(f"Scylla YAML key {root!r} is not mapping")
            merged_parent = dict(parent)
            merged_parent[child] = value
            parsed[root] = merged_parent
        return parsed

    @staticmethod
    def _apply_dotted_values(
        destination: dict[str, object], values: Mapping[str, object]
    ) -> None:
        for key, value in values.items():
            path = key.split(".")
            if len(path) == 1:
                destination[path[0]] = value
            elif len(path) == 2:
                child = CcmProvisioner._child_map(destination, path[0], create=True)
                if child is None:
                    raise OSError(f"Scylla YAML key {path[0]!r} is not mapping")
                child[path[1]] = value
            else:
                raise OSError(f"Unsupported nested Scylla YAML key {key}")

    @staticmethod
    def _verify_dotted_values(
        actual: dict[str, object] | None,
        expected: Mapping[str, object],
        description: str,
    ) -> None:
        if actual is None:
            raise OSError(f"{description} has no configuration options")
        for key, expected_value in expected.items():
            path = key.split(".")
            parent = actual
            if len(path) == 2:
                nested = CcmProvisioner._child_map(actual, path[0], create=False)
                if nested is None:
                    raise OSError(f"{description} does not preserve override {key!r}")
                parent = nested
            leaf = path[-1]
            if leaf not in parent or not CcmProvisioner._yaml_values_equal(
                parent[leaf], expected_value
            ):
                raise OSError(
                    f"{description} does not preserve override {key!r} "
                    f"(expected {expected_value!r})"
                )

    @staticmethod
    def _yaml_values_equal(actual: object, expected: object) -> bool:
        if (
            isinstance(actual, float)
            and isinstance(expected, float)
            and math.isnan(actual)
            and math.isnan(expected)
        ):
            return True
        return actual == expected

    @staticmethod
    def _child_map(
        parent: dict[str, object], key: str, *, create: bool
    ) -> dict[str, object] | None:
        existing = parent.get(key)
        if existing is None:
            if not create:
                return None
            child: dict[str, object] = {}
            parent[key] = child
            return child
        if not isinstance(existing, dict):
            if create:
                raise OSError(f"Scylla YAML key {key!r} is not mapping")
            return None
        if not all(isinstance(item, str) for item in existing):
            raise OSError(f"Scylla YAML mapping {key!r} has non-string key")
        return existing

    @staticmethod
    def _read_yaml_map(path: Path) -> dict[str, object]:
        CcmProvisioner._reject_unsafe_file(path, "configuration")
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict) or not all(
            isinstance(key, str) for key in loaded
        ):
            raise OSError(f"Expected YAML mapping in {path}")
        return loaded

    @staticmethod
    def _write_yaml_map(path: Path, contents: Mapping[str, object]) -> None:
        CcmProvisioner._reject_unsafe_file(path, "configuration")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as output:
                yaml.safe_dump(dict(contents), output, sort_keys=False)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    def _current_cluster_directory(self, ccm_directory: Path) -> Path | None:
        normalized = self._validate_ccm_directory_location(
            ccm_directory, require_exists=True
        )
        current_path = normalized / "CURRENT"
        current_state = self._path_state(current_path)
        if current_state is _PathState.ABSENT:
            return None
        if current_state is _PathState.UNKNOWN:
            raise OSError(f"Cannot determine CCM CURRENT state under {normalized}")
        self._reject_unsafe_file(current_path, "CURRENT")
        current = current_path.read_text(encoding="utf-8").strip()
        if not current:
            return None
        cluster_directory = self._owned_child(normalized, current)
        cluster_state = self._path_state(cluster_directory)
        if cluster_state is _PathState.PRESENT:
            self._validate_direct_child(normalized, cluster_directory, "cluster")
        elif cluster_state is _PathState.UNKNOWN:
            raise OSError(f"Cannot determine CCM cluster state at {cluster_directory}")
        return cluster_directory

    def _validate_cluster_metadata_if_present(
        self, cluster_directory: Path, expected_name: str
    ) -> None:
        cluster_config = cluster_directory / "cluster.conf"
        state = self._path_state(cluster_config)
        if state is _PathState.ABSENT:
            return
        if state is _PathState.UNKNOWN:
            raise OSError(
                f"Cannot determine CCM cluster configuration state at {cluster_config}"
            )
        config = self._read_yaml_map(cluster_config)
        if config.get("name") != expected_name:
            raise OSError(
                f"CCM cluster configuration has unexpected name: {cluster_config}"
            )
        configured_nodes = config.get("nodes")
        if not isinstance(configured_nodes, list):
            raise OSError(f"Invalid nodes list in {cluster_config}")
        node_names: set[str] = set()
        for configured_node in configured_nodes:
            if not isinstance(configured_node, str) or not self._is_ccm_node_name(
                configured_node
            ):
                raise OSError(
                    f"Unsafe node name in {cluster_config}: {configured_node!r}"
                )
            if configured_node in node_names:
                raise OSError(
                    f"Duplicate node name in {cluster_config}: {configured_node}"
                )
            node_names.add(configured_node)
        for node_directory in self._node_directories(cluster_directory).values():
            node_config = node_directory / "node.conf"
            node_config_state = self._path_state(node_config)
            if node_config_state is _PathState.UNKNOWN:
                raise OSError(
                    f"Cannot determine CCM node configuration state at {node_config}"
                )
            if node_config_state is _PathState.PRESENT:
                self._validate_native_node_metadata(
                    self._read_yaml_map(node_config), node_directory
                )

        configured_seeds = config.get("seeds")
        if configured_seeds is not None:
            if not isinstance(configured_seeds, list):
                raise OSError(f"Invalid seeds list in {cluster_config}")
            for seed in configured_seeds:
                if not isinstance(seed, str) or seed not in node_names:
                    raise OSError(f"Unsafe seed name in {cluster_config}: {seed!r}")

    @staticmethod
    def _validate_native_node_metadata(
        config: Mapping[str, object], node_directory: Path
    ) -> None:
        if config.get("name") != node_directory.name or "docker_id" in config:
            raise OSError(
                f"Unsafe CCM node configuration: {node_directory / 'node.conf'}"
            )

    @staticmethod
    def _path_state(path: Path) -> _PathState:
        try:
            path.lstat()
        except FileNotFoundError:
            return _PathState.ABSENT
        except OSError:
            return _PathState.UNKNOWN
        return _PathState.PRESENT

    def _remove_by_name(self, instance_id: str, ccm_directory: Path) -> None:
        normalized = self._validate_ccm_directory_location(
            ccm_directory, require_exists=False
        )
        state = self._path_state(normalized)
        if state is _PathState.ABSENT:
            return
        if state is _PathState.UNKNOWN:
            raise OSError(f"Cannot determine CCM config state at {normalized}")
        cluster_directory = self._owned_child(normalized, instance_id)
        cluster_config = cluster_directory / "cluster.conf"
        config_state = self._path_state(cluster_config)
        if config_state is _PathState.ABSENT:
            self._cleanup_absent_cluster_state(instance_id, normalized)
            return
        if config_state is _PathState.UNKNOWN:
            raise OSError(f"Cannot determine CCM cluster state for {instance_id!r}")
        self._validate_cluster_metadata_if_present(cluster_directory, instance_id)
        self._restore_orphaned_node_membership(cluster_directory)
        self._prepare_cluster_process_references(cluster_directory)
        command_error: BaseException | None = None
        try:
            self._run_ccm(
                normalized,
                ["remove", "--config-dir", str(normalized), instance_id],
            )
        except BaseException as exception:
            command_error = exception
        removed = not cluster_config.exists()
        if removed:
            if isinstance(command_error, CcmProcessCleanupError):
                raise command_error
            self._cleanup_absent_cluster_state(instance_id, normalized)
            if command_error is not None and not isinstance(
                command_error, CcmCommandError
            ):
                raise command_error
            return
        if command_error is not None:
            raise command_error
        raise OSError(f"CCM reported success but cluster {instance_id!r} still exists")

    def _remove_node_by_name(self, ccm_directory: Path, node_name: str) -> None:
        cluster_directory = self._current_cluster_directory(ccm_directory)
        if cluster_directory is None:
            return
        node_directory = self._owned_child(cluster_directory, node_name)
        cluster_config = cluster_directory / "cluster.conf"
        listed_before = self._node_listed(cluster_config, node_name)
        if not node_directory.exists() and not listed_before:
            return
        if node_directory.exists() and not listed_before:
            raise CcmProcessCleanupError(
                f"CCM node {node_name!r} has local state but is absent from membership"
            )
        self._prepare_node_process_references(cluster_directory, node_name)
        command_error: BaseException | None = None
        try:
            self._run_ccm(
                ccm_directory,
                [node_name, "remove", "--config-dir", str(ccm_directory)],
            )
        except BaseException as exception:
            command_error = exception
        listed_after = self._node_listed(cluster_config, node_name)
        node_remains = node_directory.exists()
        if node_remains and not listed_after:
            orphaned = CcmProcessCleanupError(
                f"CCM node {node_name!r} was removed from membership while local "
                "process state remains"
            )
            if command_error is not None:
                raise orphaned from command_error
            raise orphaned
        if not node_remains and not listed_after:
            if isinstance(command_error, CcmProcessCleanupError):
                raise command_error
            if command_error is not None and not isinstance(
                command_error, CcmCommandError
            ):
                raise command_error
            return
        if command_error is not None:
            raise command_error
        raise OSError(f"CCM reported success but node {node_name!r} still exists")

    @staticmethod
    def _node_listed(cluster_config: Path, node_name: str) -> bool:
        if not cluster_config.exists():
            return False
        config = CcmProvisioner._read_yaml_map(cluster_config)
        nodes = config.get("nodes")
        if not isinstance(nodes, list):
            raise OSError(f"Invalid nodes list in {cluster_config}")
        return node_name in nodes

    def _cleanup_absent_cluster_state(
        self, instance_id: str, ccm_directory: Path
    ) -> None:
        cluster_directory = self._owned_child(ccm_directory, instance_id)
        if (
            self._path_state(cluster_directory / "cluster.conf")
            is not _PathState.ABSENT
        ):
            raise OSError(f"Refusing to delete CCM-managed state for {instance_id!r}")
        cluster_state = self._path_state(cluster_directory)
        if cluster_state is _PathState.PRESENT:
            self._safe_remove_tree(cluster_directory, ccm_directory)
        elif cluster_state is _PathState.UNKNOWN:
            raise OSError(f"Cannot determine CCM cluster state at {cluster_directory}")
        current = ccm_directory / "CURRENT"
        current_state = self._path_state(current)
        if current_state is _PathState.PRESENT:
            self._reject_unsafe_file(current, "CURRENT")
            if current.read_text(encoding="utf-8").strip() == instance_id:
                current.unlink()
        elif current_state is _PathState.UNKNOWN:
            raise OSError(f"Cannot determine CCM CURRENT state at {current}")

    def _sanitize_stale_process_references(self, cluster_directory: Path) -> None:
        cluster = self._read_yaml_map(cluster_directory / "cluster.conf")
        nodes = cluster.get("nodes")
        if not isinstance(nodes, list):
            raise OSError(f"Invalid nodes list in {cluster_directory / 'cluster.conf'}")
        for value in nodes:
            if not isinstance(value, str) or not self._is_ccm_node_name(value):
                raise OSError(f"Unsafe node name in stale CCM metadata: {value!r}")
        # A failed CCM node removal can publish membership before stopping the
        # process.  Run-state recovery has already reaped exact-marker children,
        # so sanitize every remaining node directory, including such orphans,
        # before PID reuse can make a stale reference look foreign.
        for node_directory in self._node_directories(cluster_directory).values():
            node_config = node_directory / "node.conf"
            if node_config.exists():
                config = self._read_yaml_map(node_config)
                self._validate_native_node_metadata(config, node_directory)
                if "pid" in config:
                    config.pop("pid")
                    self._write_yaml_map(node_config, config)
            for pid_name in ("cassandra.pid", "scylla-jmx.pid", "scylla-agent.pid"):
                pid_file = node_directory / pid_name
                if pid_file.exists():
                    self._reject_unsafe_file(pid_file, "PID")
                    pid_file.unlink()

    def _prepare_cluster_process_references(self, cluster_directory: Path) -> bool:
        cluster = self._read_yaml_map(cluster_directory / "cluster.conf")
        configured_nodes = cluster.get("nodes")
        if not isinstance(configured_nodes, list):
            raise OSError(f"Invalid nodes list in {cluster_directory / 'cluster.conf'}")
        node_names: list[str] = []
        owned_process_remains = False
        for configured_node in configured_nodes:
            if not isinstance(configured_node, str) or not self._is_ccm_node_name(
                configured_node
            ):
                raise OSError(
                    f"Unsafe node name in {cluster_directory / 'cluster.conf'}: "
                    f"{configured_node!r}"
                )
            node_names.append(configured_node)
        node_names.extend(
            name
            for name in self._node_directories(cluster_directory)
            if name not in node_names
        )
        for node_name in node_names:
            if self._prepare_node_process_references(cluster_directory, node_name):
                owned_process_remains = True
        return owned_process_remains

    def _restore_orphaned_node_membership(self, cluster_directory: Path) -> None:
        """Make validated partial node removals visible to CCM cluster teardown."""
        cluster_config = cluster_directory / "cluster.conf"
        cluster = self._read_yaml_map(cluster_config)
        configured_nodes = cluster.get("nodes")
        if not isinstance(configured_nodes, list):
            raise OSError(f"Invalid nodes list in {cluster_config}")
        configured_names = set(configured_nodes)
        orphaned_names: list[str] = []
        for node_name, node_directory in self._node_directories(
            cluster_directory
        ).items():
            if node_name in configured_names:
                continue
            node_config_state = self._path_state(node_directory / "node.conf")
            if node_config_state is _PathState.PRESENT:
                orphaned_names.append(node_name)
                continue
            if node_config_state is _PathState.UNKNOWN:
                raise OSError(
                    f"Cannot determine CCM node configuration state at "
                    f"{node_directory / 'node.conf'}"
                )
            if self._prepare_node_process_references(cluster_directory, node_name):
                raise CcmProcessCleanupError(
                    f"Cannot restore orphaned CCM node {node_name!r} without "
                    "node.conf while owned processes remain"
                )
        if orphaned_names:
            cluster["nodes"] = [*configured_nodes, *orphaned_names]
            self._write_yaml_map(cluster_config, cluster)

    def _node_directories(self, cluster_directory: Path) -> dict[str, Path]:
        """Return every validated native CCM node directory in numeric order."""
        directories: dict[str, Path] = {}
        for candidate in cluster_directory.iterdir():
            if not self._is_ccm_node_name(candidate.name):
                continue
            state = self._path_state(candidate)
            if state is _PathState.ABSENT:
                continue
            if state is _PathState.UNKNOWN:
                raise OSError(f"Cannot determine CCM node state at {candidate}")
            self._validate_direct_child(cluster_directory, candidate, "node")
            directories[candidate.name] = candidate
        return dict(sorted(directories.items(), key=lambda item: int(item[0][4:])))

    def _prepare_node_process_references(
        self, cluster_directory: Path, node_name: str
    ) -> bool:
        if not self._is_ccm_node_name(node_name):
            raise OSError(f"Unsafe CCM node name: {node_name!r}")
        node_directory = self._owned_child(cluster_directory, node_name)
        if not node_directory.exists():
            return False
        self._validate_direct_child(cluster_directory, node_directory, "node")

        node_config = node_directory / "node.conf"
        configured_pid: int | None = None
        configured_state = _ProcessReferenceState.ABSENT
        node_yaml: dict[str, object] | None = None
        if node_config.exists():
            node_yaml = self._read_yaml_map(node_config)
            self._validate_native_node_metadata(node_yaml, node_directory)
            if "pid" in node_yaml:
                configured_pid = self._parse_pid_reference(
                    node_yaml["pid"], node_config
                )
                configured_state = self._inspect_process_reference(
                    configured_pid,
                    node_config,
                    node_directory,
                    _ProcessReferenceKind.SCYLLA,
                )

        scylla_pid_file = node_directory / "cassandra.pid"
        scylla_pid = self._read_pid_reference(scylla_pid_file)
        scylla_state = self._inspect_process_reference(
            scylla_pid,
            scylla_pid_file,
            node_directory,
            _ProcessReferenceKind.SCYLLA,
        )
        if scylla_state is _ProcessReferenceState.OWNED and (
            configured_state is not _ProcessReferenceState.OWNED
            or scylla_pid != configured_pid
        ):
            raise CcmProcessCleanupError(
                f"CCM cannot safely stop Scylla PID {scylla_pid}; node.conf "
                "does not reference same owned process"
            )

        jmx_pid_file = node_directory / "scylla-jmx.pid"
        jmx_state = self._inspect_process_reference(
            self._read_pid_reference(jmx_pid_file),
            jmx_pid_file,
            node_directory,
            _ProcessReferenceKind.JMX,
        )
        agent_pid_file = node_directory / "scylla-agent.pid"
        agent_state = self._inspect_process_reference(
            self._read_pid_reference(agent_pid_file),
            agent_pid_file,
            node_directory,
            _ProcessReferenceKind.AGENT,
        )

        if configured_state is _ProcessReferenceState.DEAD and node_yaml is not None:
            node_yaml.pop("pid", None)
            self._write_yaml_map(node_config, node_yaml)
        self._delete_dead_pid_reference(scylla_pid_file, scylla_state)
        self._delete_dead_pid_reference(jmx_pid_file, jmx_state)
        self._delete_dead_pid_reference(agent_pid_file, agent_state)
        return any(
            state is _ProcessReferenceState.OWNED
            for state in (configured_state, scylla_state, jmx_state, agent_state)
        )

    def _inspect_process_reference(
        self,
        pid: int | None,
        source: Path,
        node_directory: Path,
        kind: _ProcessReferenceKind,
    ) -> _ProcessReferenceState:
        if pid is None:
            return _ProcessReferenceState.ABSENT
        process_directory = Path("/proc") / str(pid)
        try:
            start_ticks = self._read_process_start_ticks(pid)
        except (FileNotFoundError, ProcessLookupError):
            return _ProcessReferenceState.DEAD
        try:
            if process_directory.stat().st_uid not in {os.getuid(), os.geteuid()}:
                raise CcmProcessCleanupError(
                    f"CCM process reference {source} points to foreign live PID {pid}"
                )
            environment = (process_directory / "environ").read_bytes()
            marker = f"SCYLLA_CCM_RUN_DIR={self._run_directory}".encode()
            if marker not in environment.split(b"\0"):
                if not self._same_live_process(pid, start_ticks):
                    return _ProcessReferenceState.DEAD
                raise CcmProcessCleanupError(
                    f"CCM process reference {source} points to unrelated live PID {pid}"
                )
            arguments = self._read_process_arguments(process_directory / "cmdline")
            if not self._matches_expected_process(
                arguments, node_directory, kind
            ) or not self._matches_expected_executable(
                process_directory, arguments, kind
            ):
                if not self._same_live_process(pid, start_ticks):
                    return _ProcessReferenceState.DEAD
                raise CcmProcessCleanupError(
                    f"CCM process reference {source} points to wrong "
                    f"{kind.value} process {pid}"
                )
            if not self._same_live_process(pid, start_ticks):
                return _ProcessReferenceState.DEAD
            return _ProcessReferenceState.OWNED
        except CcmProcessCleanupError:
            raise
        except (OSError, UnicodeError) as exception:
            if not self._same_live_process(pid, start_ticks):
                return _ProcessReferenceState.DEAD
            raise CcmProcessCleanupError(
                f"Cannot prove ownership of live PID {pid} referenced by {source}"
            ) from exception

    @staticmethod
    def _matches_expected_process(
        arguments: Sequence[str],
        node_directory: Path,
        kind: _ProcessReferenceKind,
    ) -> bool:
        if not arguments:
            return False
        node = node_directory.absolute()
        executable = Path(arguments[0])
        if kind is _ProcessReferenceKind.SCYLLA:
            return executable == node / "bin/scylla"
        if kind is _ProcessReferenceKind.JMX:
            expected_jar = str(node / "bin/scylla-jmx-1.0.jar")
            expected_launcher = executable == node / "bin/symlinks/scylla-jmx"
            return (
                expected_launcher or executable.name == "java"
            ) and CcmProvisioner._contains_argument_pair(
                arguments, "-jar", expected_jar
            )
        return (
            executable.is_absolute()
            and executable.name == "scylla-manager-agent"
            and (
                CcmProvisioner._contains_argument_pair(
                    arguments,
                    "--config-file",
                    str(node / "conf/scylla-manager-agent.yaml"),
                )
            )
        )

    @staticmethod
    def _matches_expected_executable(
        process_directory: Path,
        arguments: Sequence[str],
        kind: _ProcessReferenceKind,
    ) -> bool:
        if (
            kind is not _ProcessReferenceKind.JMX
            or not arguments
            or Path(arguments[0]).name != "java"
        ):
            return True
        executable = os.readlink(process_directory / "exe")
        return Path(executable).name in {"java", "java (deleted)"}

    @staticmethod
    def _contains_argument_pair(
        arguments: Sequence[str], option: str, expected_value: str
    ) -> bool:
        return any(
            arguments[index] == option and arguments[index + 1] == expected_value
            for index in range(len(arguments) - 1)
        )

    @staticmethod
    def _read_process_arguments(path: Path) -> tuple[str, ...]:
        return tuple(
            value.decode("utf-8") for value in path.read_bytes().split(b"\0") if value
        )

    @staticmethod
    def _read_process_start_ticks(pid: int) -> int:
        value = (Path("/proc") / str(pid) / "stat").read_text(encoding="ascii")
        command_end = value.rfind(")")
        if command_end < 0 or command_end + 2 >= len(value):
            raise OSError(f"Cannot parse process identity for PID {pid}")
        fields = value[command_end + 2 :].split()
        if len(fields) <= 19:
            raise OSError(f"Cannot parse process identity for PID {pid}")
        try:
            return int(fields[19])
        except ValueError as exception:
            raise OSError(f"Cannot parse process identity for PID {pid}") from exception

    @staticmethod
    def _same_live_process(pid: int, expected_start_ticks: int) -> bool:
        try:
            stat_value = (Path("/proc") / str(pid) / "stat").read_text(encoding="ascii")
            command_end = stat_value.rfind(")")
            state = stat_value[command_end + 2 :].split()[0]
            return (
                state != "Z"
                and CcmProvisioner._read_process_start_ticks(pid)
                == expected_start_ticks
            )
        except (OSError, IndexError):
            return False

    @staticmethod
    def _read_pid_reference(path: Path) -> int | None:
        try:
            path.lstat()
        except FileNotFoundError:
            return None
        CcmProvisioner._reject_unsafe_file(path, "PID")
        return CcmProvisioner._parse_pid_reference(
            path.read_text(encoding="ascii").strip(), path
        )

    @staticmethod
    def _parse_pid_reference(value: object, source: Path) -> int:
        text = str(value)
        if not text.isascii() or not text.isdecimal():
            raise OSError(f"Invalid process ID in {source}")
        pid = int(text)
        if pid < 2 or str(pid) != text:
            raise OSError(f"Invalid process ID in {source}")
        return pid

    @staticmethod
    def _delete_dead_pid_reference(path: Path, state: _ProcessReferenceState) -> None:
        if state is not _ProcessReferenceState.DEAD:
            return
        CcmProvisioner._reject_unsafe_file(path, "PID")
        path.unlink()

    @staticmethod
    def _copy_optional_diagnostic(
        source: Path, destination: Path, relative: Path
    ) -> None:
        state = CcmProvisioner._path_state(source)
        if state is _PathState.PRESENT:
            CcmProvisioner._copy_diagnostic(source, destination, relative)
        elif state is _PathState.UNKNOWN:
            raise OSError(f"Cannot determine diagnostic source state at {source}")

    @staticmethod
    def _copy_volatile_diagnostic(
        source: Path, destination: Path, relative: Path
    ) -> None:
        with contextlib.suppress(FileNotFoundError):
            CcmProvisioner._copy_diagnostic(source, destination, relative)

    @staticmethod
    def _copy_diagnostic(source: Path, destination: Path, relative: Path) -> None:
        source_mode = source.lstat().st_mode
        if not stat.S_ISREG(source_mode):
            raise OSError(f"Refusing non-regular CCM diagnostic source file {source}")
        if CcmProvisioner._is_private_key_file(source):
            return
        target = (destination / relative).absolute()
        if not CcmProvisioner._is_relative_to(target, destination.absolute()):
            raise OSError(f"Refusing diagnostic path outside {destination}")
        CcmProvisioner._prepare_diagnostic_directory(destination, relative.parent)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
        )
        temporary = Path(temporary_name)
        try:
            with (
                source.open("rb") as input_file,
                os.fdopen(descriptor, "wb") as output_file,
            ):
                shutil.copyfileobj(input_file, output_file)
                output_file.flush()
                os.fsync(output_file.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _prepare_diagnostic_directory(destination: Path, relative: Path) -> Path:
        current = destination.absolute()
        CcmProvisioner._reject_unsafe_directory(current, "diagnostic")
        for component in relative.parts:
            if component in ("", "."):
                continue
            child = CcmProvisioner._owned_child(current, component)
            state = CcmProvisioner._path_state(child)
            if state is _PathState.ABSENT:
                child.mkdir()
            elif state is _PathState.UNKNOWN:
                raise OSError(f"Cannot determine diagnostic directory state at {child}")
            CcmProvisioner._validate_direct_child(current, child, "diagnostic")
            current = child
        return current

    def _validate_ccm_directory_location(
        self, ccm_directory: Path, *, require_exists: bool
    ) -> Path:
        normalized = ccm_directory.absolute()
        if normalized.parent != self._clusters_directory:
            raise OSError(f"Refusing CCM config outside run state: {ccm_directory}")
        state = self._path_state(normalized)
        if state is _PathState.ABSENT:
            if require_exists:
                raise OSError(f"CCM config directory does not exist: {ccm_directory}")
            return normalized
        if state is _PathState.UNKNOWN:
            raise OSError(f"Cannot determine CCM config state: {ccm_directory}")
        self._validate_direct_child(self._clusters_directory, normalized, "CCM config")
        return normalized

    @staticmethod
    def _prepare_directory(directory: Path, parent: Path, description: str) -> None:
        directory.mkdir(exist_ok=True)
        CcmProvisioner._validate_direct_child(parent, directory, description)

    @staticmethod
    def _validate_direct_child(parent: Path, child: Path, description: str) -> None:
        normalized_parent = parent.absolute()
        normalized_child = child.absolute()
        if normalized_child.parent != normalized_parent:
            raise OSError(f"Refusing unsafe CCM {description} directory {child}")
        CcmProvisioner._reject_unsafe_directory(normalized_child, description)
        if normalized_child.resolve(strict=True).parent != normalized_parent.resolve(
            strict=True
        ):
            raise OSError(f"Refusing unsafe CCM {description} directory {child}")

    @staticmethod
    def _reject_unsafe_directory(path: Path, description: str) -> None:
        if path.is_symlink() or not path.is_dir():
            raise OSError(f"Refusing unsafe CCM {description} directory {path}")

    @staticmethod
    def _reject_unsafe_file(path: Path, description: str) -> None:
        if path.is_symlink() or not path.is_file():
            raise OSError(f"Refusing non-regular CCM {description} file {path}")

    @staticmethod
    def _owned_child(parent: Path, child: str) -> Path:
        result = (parent / child).absolute()
        if result.parent != parent.absolute() or child in ("", ".", ".."):
            raise OSError(f"Refusing path outside CCM directory: {result}")
        return result

    @staticmethod
    def _safe_remove_tree(path: Path, parent: Path) -> None:
        CcmProvisioner._validate_direct_child(parent, path, "cleanup")
        if path.is_symlink():
            raise OSError(f"Refusing to recursively delete symlink {path}")
        shutil.rmtree(path)

    @staticmethod
    def _is_private_key_file(path: Path) -> bool:
        return path.suffix.lower() in (".key", ".pem")

    @staticmethod
    def _is_ccm_node_name(name: str) -> bool:
        return name.startswith("node") and name[4:].isdigit() and int(name[4:]) > 0

    @staticmethod
    def _is_relative_to(path: Path, parent: Path) -> bool:
        try:
            path.relative_to(parent)
            return True
        except ValueError:
            return False
