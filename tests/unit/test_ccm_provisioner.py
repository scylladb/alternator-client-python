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

"""Focused tests for native CCM provisioning commands and recovery boundaries."""

from __future__ import annotations

import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
import yaml

from tests.testinfra.ccm_provisioner import (
    GOSSIPING_PROPERTY_FILE_SNITCH,
    HTTP_PORT,
    HTTPS_PORT,
    TEST_SALTED_PASSWORD,
    TEST_USER,
    CcmClusterProvisioningError,
    CcmCommandError,
    CcmNodeProvisioningError,
    CcmProcessCleanupError,
    CcmProvisioner,
    requires_next_run_recovery,
)
from tests.testinfra.cluster import PhysicalTestCluster, TestClusterNode
from tests.testinfra.cluster_spec import (
    AlternatorTransport,
    ClusterSecuritySpec,
    ClusterSpec,
    ClusterTopology,
    DatacenterSpec,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


def make_provisioner(tmp_path: Path) -> CcmProvisioner:
    """Create non-operational provisioner rooted entirely below tmp_path."""
    return CcmProvisioner(
        tmp_path / "run",
        tmp_path / "diagnostics",
        "fake-ccm",
        command_timeout=2.0,
        readiness_timeout=1.0,
    )


def make_ccm_directory(provisioner: CcmProvisioner, name: str = "cluster") -> Path:
    """Create one valid provisioner-owned CCM configuration directory."""
    directory = provisioner.run_directory / "clusters" / name
    directory.mkdir()
    return directory


class RecordingProvisioner(CcmProvisioner):
    """Provisioner that records external commands without spawning children."""

    def __init__(self, tmp_path: Path) -> None:
        super().__init__(
            tmp_path / "run",
            tmp_path / "diagnostics",
            "fake-ccm",
            command_timeout=2.0,
            readiness_timeout=1.0,
        )
        self.ccm_commands: list[tuple[str, ...]] = []
        self.commands: list[tuple[str, ...]] = []

    def _run_ccm(self, ccm_directory: Path, arguments: Sequence[str]) -> None:
        self.ccm_commands.append(tuple(arguments))

    def _run_command(self, ccm_directory: Path, command: Sequence[str]) -> None:
        self.commands.append(tuple(command))

    def apply_yaml_overrides(
        self,
        spec: ClusterSpec,
        ccm_directory: Path,
        nodes: Sequence[TestClusterNode],
        *,
        update_cluster_state: bool,
    ) -> None:
        return


def test_provision_exempts_all_nodes_from_proxies_before_ccm_commands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    node_hosts = {"127.0.7.1", "127.0.7.2", "127.0.7.3"}
    monkeypatch.setenv("NO_PROXY", "localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1")

    class ProxyAwareProvisioner(RecordingProvisioner):
        def _run_ccm(self, ccm_directory: Path, arguments: Sequence[str]) -> None:
            assert node_hosts <= set(os.environ["NO_PROXY"].split(","))
            assert node_hosts <= set(os.environ["no_proxy"].split(","))
            super()._run_ccm(ccm_directory, arguments)

        def start(
            self,
            cluster: PhysicalTestCluster,
            nodes: Sequence[TestClusterNode] | None = None,
        ) -> None:
            return

    provisioner = ProxyAwareProvisioner(tmp_path)
    spec = ClusterSpec().with_transports(AlternatorTransport.HTTP)

    cluster = provisioner.provision(spec, "cluster", 7)

    assert {node.address for node in cluster.nodes} == node_hosts


def test_provision_callback_runs_after_cluster_start(tmp_path: Path) -> None:
    events: list[str] = []

    class CallbackProvisioner(RecordingProvisioner):
        def start(
            self,
            cluster: PhysicalTestCluster,
            nodes: Sequence[TestClusterNode] | None = None,
        ) -> None:
            events.append("started")

    provisioner = CallbackProvisioner(tmp_path)
    published: list[PhysicalTestCluster] = []

    def publish(provisioned: PhysicalTestCluster) -> None:
        events.append("published")
        published.append(provisioned)

    cluster = provisioner.provision(
        ClusterSpec().with_transports(AlternatorTransport.HTTP),
        "cluster",
        7,
        on_provisioned=publish,
    )

    assert events == ["started", "published"]
    assert published == [cluster]


def test_provision_callback_failure_runs_rollback_and_propagates(
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class CallbackFailure(RuntimeError):
        pass

    class CallbackProvisioner(RecordingProvisioner):
        def start(
            self,
            cluster: PhysicalTestCluster,
            nodes: Sequence[TestClusterNode] | None = None,
        ) -> None:
            events.append("started")

        def _remove_by_name(self, instance_id: str, ccm_directory: Path) -> None:
            events.append("rolled back")

    provisioner = CallbackProvisioner(tmp_path)
    failure = CallbackFailure("publication interrupted")

    def fail_publication(_cluster: PhysicalTestCluster) -> None:
        events.append("publication attempted")
        raise failure

    with pytest.raises(CallbackFailure) as raised:
        provisioner.provision(
            ClusterSpec().with_transports(AlternatorTransport.HTTP),
            "cluster",
            7,
            on_provisioned=fail_publication,
        )

    assert raised.value is failure
    assert events == ["started", "publication attempted", "rolled back"]


def test_topology_maps_to_ccm_multidc_and_additional_rack_commands(
    tmp_path: Path,
) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory = make_ccm_directory(provisioner)
    spec = ClusterSpec(
        topology=ClusterTopology(
            (DatacenterSpec.create(2, 1), DatacenterSpec.create(1, 2))
        )
    )

    nodes = provisioner._build_nodes(spec, 23)  # noqa: SLF001 -- command contract
    provisioner._add_additional_rack_nodes(  # noqa: SLF001 -- command contract
        spec, nodes, ccm_directory
    )

    assert provisioner._first_rack_counts(spec) == "2:1"  # noqa: SLF001 -- focused command contract
    assert [
        (node.name, node.address, node.datacenter, node.rack) for node in nodes
    ] == [
        ("node1", "127.0.23.1", "dc1", "RAC1"),
        ("node2", "127.0.23.2", "dc1", "RAC1"),
        ("node3", "127.0.23.3", "dc2", "RAC1"),
        ("node4", "127.0.23.4", "dc1", "RAC2"),
        ("node5", "127.0.23.5", "dc2", "RAC2"),
        ("node6", "127.0.23.6", "dc2", "RAC2"),
    ]
    assert provisioner.ccm_commands == [
        (
            "add",
            "--config-dir",
            str(ccm_directory),
            "node4",
            "--scylla",
            "--seeds",
            "--itf",
            "127.0.23.4",
            "--data-center",
            "dc1",
            "--rack",
            "RAC2",
        ),
        (
            "add",
            "--config-dir",
            str(ccm_directory),
            "node5",
            "--scylla",
            "--seeds",
            "--itf",
            "127.0.23.5",
            "--data-center",
            "dc2",
            "--rack",
            "RAC2",
        ),
        (
            "add",
            "--config-dir",
            str(ccm_directory),
            "node6",
            "--scylla",
            "--seeds",
            "--itf",
            "127.0.23.6",
            "--data-center",
            "dc2",
            "--rack",
            "RAC2",
        ),
    ]
    assert all(
        "--auto-bootstrap" not in command for command in provisioner.ccm_commands
    )


def test_configure_uses_sorted_typed_options_and_exact_security_backends(
    tmp_path: Path,
) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory = make_ccm_directory(provisioner)
    spec = ClusterSpec().with_security(ClusterSecuritySpec.ENFORCED)
    nodes = provisioner._build_nodes(spec, 7)  # noqa: SLF001 -- command contract

    provisioner._configure(spec, nodes, ccm_directory)  # noqa: SLF001 -- focused command contract

    command = provisioner.ccm_commands[-1]
    assert command[:3] == ("updateconf", "--config-dir", str(ccm_directory))
    options = command[3:]
    assert options == tuple(sorted(options))
    assert f"alternator_port:{HTTP_PORT}" in options
    assert f"alternator_https_port:{HTTPS_PORT}" in options
    assert "authenticator:org.apache.cassandra.auth.PasswordAuthenticator" in options
    assert "authorizer:org.apache.cassandra.auth.CassandraAuthorizer" in options
    assert f"auth_superuser_name:{TEST_USER}" in options
    assert f"auth_superuser_salted_password:{TEST_SALTED_PASSWORD}" in options
    assert "alternator_enforce_authorization:true" in options
    assert f"endpoint_snitch:{GOSSIPING_PROPERTY_FILE_SNITCH}" in options


def test_tls_material_uses_rsa3072_and_per_node_ip_san(tmp_path: Path) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory = make_ccm_directory(provisioner)
    node = TestClusterNode("node1", "127.0.4.1", "dc1", "RAC1")

    ca_path = provisioner._create_certificate_authority(  # noqa: SLF001 -- focused TLS contract
        "cluster", ccm_directory
    )
    provisioner._configure_node_certificate(  # noqa: SLF001 -- focused TLS contract
        node, ccm_directory, ca_path
    )

    assert ca_path == ccm_directory / "tls/ca.crt"
    assert len(provisioner.commands) == 3
    assert "rsa:3072" in provisioner.commands[0]
    assert "rsa:3072" in provisioner.commands[1]
    extension = ccm_directory / "tls/node1/server.ext"
    assert "subjectAltName=IP:127.0.4.1" in extension.read_text(encoding="ascii")
    node_update = provisioner.ccm_commands[-1]
    assert node_update[:3] == ("node1", "updateconf", "--config-dir")
    assert any(
        argument.startswith("alternator_encryption_options.certificate:")
        for argument in node_update
    )
    assert any(
        argument.startswith("alternator_encryption_options.keyfile:")
        for argument in node_update
    )


def prepare_yaml_cluster(
    provisioner: CcmProvisioner, spec: ClusterSpec
) -> tuple[Path, list[TestClusterNode]]:
    """Write minimal CCM metadata and Scylla YAML for override tests."""
    ccm_directory = make_ccm_directory(provisioner)
    (ccm_directory / "CURRENT").write_text("cluster\n", encoding="ascii")
    cluster_directory = ccm_directory / "cluster"
    cluster_directory.mkdir()
    nodes = provisioner._build_nodes(spec, 9)  # noqa: SLF001 -- fixture setup
    (cluster_directory / "cluster.conf").write_text(
        yaml.safe_dump(
            {
                "name": "cluster",
                "nodes": [node.name for node in nodes],
                "config_options": {"hinted_handoff_enabled": True},
            }
        ),
        encoding="utf-8",
    )
    for node in nodes:
        node_directory = cluster_directory / node.name
        (node_directory / "conf").mkdir(parents=True)
        (node_directory / "node.conf").write_text(
            yaml.safe_dump({"config_options": {"hinted_handoff_enabled": True}}),
            encoding="utf-8",
        )
        (node_directory / "conf/scylla.yaml").write_text(
            yaml.safe_dump({"hinted_handoff_enabled": True}), encoding="utf-8"
        )
    return ccm_directory, nodes


def test_yaml_overrides_are_applied_atomically_and_verified(tmp_path: Path) -> None:
    provisioner = make_provisioner(tmp_path)
    spec = (
        ClusterSpec()
        .with_yaml_override("hinted_handoff_enabled", "false")
        .with_yaml_override("experimental_features", "null")
        .with_yaml_override("object_storage_config.bucket", '"test-bucket"')
    )
    ccm_directory, nodes = prepare_yaml_cluster(provisioner, spec)

    provisioner.apply_yaml_overrides(
        spec, ccm_directory, nodes, update_cluster_state=True
    )
    provisioner.verify_yaml_overrides(spec, ccm_directory, nodes)

    cluster = yaml.safe_load(
        (ccm_directory / "cluster/cluster.conf").read_text(encoding="utf-8")
    )
    assert cluster["config_options"]["hinted_handoff_enabled"] is False
    node = yaml.safe_load(
        (ccm_directory / "cluster/node1/conf/scylla.yaml").read_text(encoding="utf-8")
    )
    assert node["hinted_handoff_enabled"] is False
    assert node["experimental_features"] is None
    assert node["object_storage_config"]["bucket"] == "test-bucket"


def test_parent_and_dotted_yaml_overrides_are_merged_before_verification(
    tmp_path: Path,
) -> None:
    provisioner = make_provisioner(tmp_path)
    spec = (
        ClusterSpec()
        .with_yaml_override("custom_group", "{existing: true}")
        .with_yaml_override("custom_group.child", "42")
    )
    ccm_directory, nodes = prepare_yaml_cluster(provisioner, spec)

    provisioner.apply_yaml_overrides(
        spec, ccm_directory, nodes, update_cluster_state=True
    )
    provisioner.verify_yaml_overrides(spec, ccm_directory, nodes)

    node = yaml.safe_load(
        (ccm_directory / "cluster/node1/conf/scylla.yaml").read_text(encoding="utf-8")
    )
    assert node["custom_group"] == {"existing": True, "child": 42}


def test_yaml_nan_override_can_be_verified(tmp_path: Path) -> None:
    """YAML NaN values use semantic equality across independent parses."""
    provisioner = make_provisioner(tmp_path)
    spec = ClusterSpec().with_yaml_override("custom_value", ".nan")
    ccm_directory, nodes = prepare_yaml_cluster(provisioner, spec)

    provisioner.apply_yaml_overrides(
        spec, ccm_directory, nodes, update_cluster_state=True
    )
    provisioner.verify_yaml_overrides(spec, ccm_directory, nodes)


def test_external_command_writes_durable_per_command_and_aggregate_logs(
    tmp_path: Path,
) -> None:
    provisioner = make_provisioner(tmp_path)
    ccm_directory = make_ccm_directory(provisioner)

    provisioner._run_command(  # noqa: SLF001 -- subprocess contract
        ccm_directory,
        [
            sys.executable,
            "-c",
            "import os; print(os.environ['SCYLLA_CCM_RUN_DIR'])",
        ],
    )

    logs = list(ccm_directory.glob("ccm-command-*.log"))
    assert len(logs) == 1
    output = logs[0].read_text(encoding="utf-8")
    assert str(provisioner.run_directory) in output
    assert "[exit 0]" in output
    assert output in (ccm_directory / "ccm-commands.log").read_text(encoding="utf-8")


def test_relative_ccm_override_survives_command_working_directory_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "custom-ccm"
    executable.write_text(
        f"#!{sys.executable}\nprint('relative CCM override ran')\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SCYLLA_CCM_PATH", "./custom-ccm")
    provisioner = CcmProvisioner(
        tmp_path / "run",
        tmp_path / "diagnostics",
        command_timeout=2.0,
        readiness_timeout=1.0,
    )
    ccm_directory = make_ccm_directory(provisioner)

    provisioner._run_ccm(  # noqa: SLF001 -- executable resolution contract
        ccm_directory, ["create", "--help"]
    )

    aggregate = (ccm_directory / "ccm-commands.log").read_text(encoding="utf-8")
    assert "relative CCM override ran" in aggregate


def test_empty_ccm_override_uses_default_executable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SCYLLA_CCM_PATH", "")
    monkeypatch.setattr(
        "tests.testinfra.ccm_provisioner.shutil.which", lambda _executable: None
    )

    provisioner = CcmProvisioner(
        tmp_path / "run",
        tmp_path / "diagnostics",
        command_timeout=2.0,
        readiness_timeout=1.0,
    )

    assert provisioner._ccm_executable == "ccm"  # noqa: SLF001 -- configuration contract


def test_nonzero_command_is_typed_and_log_survives(tmp_path: Path) -> None:
    provisioner = make_provisioner(tmp_path)
    ccm_directory = make_ccm_directory(provisioner)

    with pytest.raises(CcmCommandError) as raised:
        provisioner._run_command(  # noqa: SLF001 -- subprocess contract
            ccm_directory,
            [sys.executable, "-c", "print('failure marker'); raise SystemExit(7)"],
        )

    assert raised.value.exit_code == 7
    assert "failure marker" in raised.value.output
    assert "[exit 7]" in raised.value.output


def test_command_runner_is_linux_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provisioner = make_provisioner(tmp_path)
    ccm_directory = make_ccm_directory(provisioner)
    monkeypatch.setattr(sys, "platform", "darwin")

    with pytest.raises(NotImplementedError, match="Linux only"):
        provisioner._run_command(  # noqa: SLF001 -- platform contract
            ccm_directory, [sys.executable, "-c", "pass"]
        )


def test_diagnostics_copy_logs_and_configs_but_never_private_keys(
    tmp_path: Path,
) -> None:
    provisioner = make_provisioner(tmp_path)
    spec = ClusterSpec().with_yaml_override("hinted_handoff_enabled", "false")
    ccm_directory, _nodes = prepare_yaml_cluster(provisioner, spec)
    (ccm_directory / "ccm-command-one.log").write_text("command", encoding="utf-8")
    logs = ccm_directory / "cluster/node1/logs"
    logs.mkdir()
    (logs / "system.log").write_text("diagnostic", encoding="utf-8")
    (logs / "secret.key").write_text("private", encoding="utf-8")

    provisioner.collect_diagnostics("cluster", ccm_directory)

    destination = provisioner.diagnostics_directory / "cluster"
    assert (destination / "ccm-command-one.log").read_text() == "command"
    assert (destination / "cluster/node1/logs/system.log").read_text() == "diagnostic"
    assert not (destination / "cluster/node1/logs/secret.key").exists()


def test_diagnostics_reject_symlinked_node_configuration_directory(
    tmp_path: Path,
) -> None:
    provisioner = make_provisioner(tmp_path)
    ccm_directory, _nodes = prepare_yaml_cluster(provisioner, ClusterSpec())
    configuration = ccm_directory / "cluster/node1/conf"
    (configuration / "scylla.yaml").unlink()
    configuration.rmdir()
    external_configuration = tmp_path / "external-configuration"
    external_configuration.mkdir()
    (external_configuration / "scylla.yaml").write_text(
        "external: marker\n", encoding="utf-8"
    )
    configuration.symlink_to(external_configuration, target_is_directory=True)

    with pytest.raises(OSError, match="unsafe CCM node configuration"):
        provisioner.collect_diagnostics("cluster", ccm_directory)

    copied_configuration = (
        provisioner.diagnostics_directory / "cluster/cluster/node1/conf/scylla.yaml"
    )
    assert not copied_configuration.exists()


def test_diagnostics_reject_destination_symlink_before_creating_children(
    tmp_path: Path,
) -> None:
    provisioner = make_provisioner(tmp_path)
    ccm_directory, _nodes = prepare_yaml_cluster(provisioner, ClusterSpec())
    destination = provisioner.diagnostics_directory / "cluster"
    destination.mkdir()
    external = tmp_path / "external-diagnostics"
    external.mkdir()
    (destination / "cluster").symlink_to(external, target_is_directory=True)

    with pytest.raises(OSError, match="unsafe CCM diagnostic"):
        provisioner.collect_diagnostics("cluster", ccm_directory)

    assert not (external / "node1").exists()


def test_diagnostics_tolerate_rotated_command_and_node_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provisioner = make_provisioner(tmp_path)
    ccm_directory, _nodes = prepare_yaml_cluster(provisioner, ClusterSpec())
    command_log = ccm_directory / "ccm-command-rotated.log"
    command_log.write_text("command", encoding="utf-8")
    logs = ccm_directory / "cluster/node1/logs"
    logs.mkdir()
    node_log = logs / "system.log"
    node_log.write_text("node", encoding="utf-8")
    copy_diagnostic = CcmProvisioner._copy_diagnostic  # noqa: SLF001 -- race injection

    def rotate_before_copy(source: Path, destination: Path, relative: Path) -> None:
        if source in {command_log, node_log}:
            source.unlink()
        copy_diagnostic(source, destination, relative)

    monkeypatch.setattr(CcmProvisioner, "_copy_diagnostic", rotate_before_copy)

    provisioner.collect_diagnostics("cluster", ccm_directory)

    destination = provisioner.diagnostics_directory / "cluster/cluster"
    assert (destination / "cluster.conf").is_file()
    assert (destination / "node1/node.conf").is_file()
    assert (destination / "node1/conf/scylla.yaml").is_file()


def test_diagnostics_do_not_tolerate_disappearing_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provisioner = make_provisioner(tmp_path)
    ccm_directory, _nodes = prepare_yaml_cluster(provisioner, ClusterSpec())
    cluster_config = ccm_directory / "cluster/cluster.conf"
    copy_diagnostic = CcmProvisioner._copy_diagnostic  # noqa: SLF001 -- race injection

    def remove_before_copy(source: Path, destination: Path, relative: Path) -> None:
        if source == cluster_config:
            source.unlink()
        copy_diagnostic(source, destination, relative)

    monkeypatch.setattr(CcmProvisioner, "_copy_diagnostic", remove_before_copy)

    with pytest.raises(FileNotFoundError):
        provisioner.collect_diagnostics("cluster", ccm_directory)


class DiagnosticFailureProvisioner(RecordingProvisioner):
    """Removal probe proving snapshot failure retains live state."""

    def __init__(self, tmp_path: Path) -> None:
        super().__init__(tmp_path)
        self.removal_attempted = False

    def collect_diagnostics(self, instance_id: str, ccm_directory: Path) -> None:
        raise OSError("diagnostic destination unavailable")

    def _remove_by_name(self, instance_id: str, ccm_directory: Path) -> None:
        self.removal_attempted = True


def test_diagnostic_snapshot_failure_blocks_destructive_removal(tmp_path: Path) -> None:
    provisioner = DiagnosticFailureProvisioner(tmp_path)
    ccm_directory = make_ccm_directory(provisioner)
    cluster = PhysicalTestCluster(
        provisioner,
        "cluster",
        3,
        ccm_directory,
        ClusterSpec(),
        [TestClusterNode("node1", "127.0.3.1", "dc1", "RAC1")],
        None,
        None,
    )

    with pytest.raises(OSError, match="diagnostic destination unavailable"):
        provisioner.remove(cluster)
    assert not provisioner.removal_attempted


class FailedNodeProvisioner(RecordingProvisioner):
    """Backend whose node start fails and whose local rollback succeeds."""

    def __init__(self, tmp_path: Path) -> None:
        super().__init__(tmp_path)
        self.removed_node: str | None = None

    def _run_ccm(self, ccm_directory: Path, arguments: Sequence[str]) -> None:
        self.ccm_commands.append(tuple(arguments))
        if "start" in arguments:
            raise OSError("start failed")

    def is_node_running(
        self, cluster: PhysicalTestCluster, node: TestClusterNode
    ) -> bool:
        return False

    def collect_diagnostics(self, instance_id: str, ccm_directory: Path) -> None:
        return

    def verify_yaml_overrides(
        self,
        spec: ClusterSpec,
        ccm_directory: Path,
        nodes: Sequence[TestClusterNode],
    ) -> None:
        return

    def _remove_node_by_name(self, ccm_directory: Path, node_name: str) -> None:
        self.removed_node = node_name


def test_failed_node_start_gets_one_bounded_rollback_with_ambiguous_membership(
    tmp_path: Path,
) -> None:
    provisioner = FailedNodeProvisioner(tmp_path)
    ccm_directory = make_ccm_directory(provisioner)
    cluster = PhysicalTestCluster(
        provisioner,
        "cluster",
        5,
        ccm_directory,
        ClusterSpec().with_transports(AlternatorTransport.HTTP),
        [TestClusterNode("node1", "127.0.5.1", "dc1", "RAC1")],
        None,
        None,
    )

    with pytest.raises(CcmNodeProvisioningError) as raised:
        provisioner.add_node(cluster, "dc1", "RAC2")

    assert provisioner.removed_node == "node2"
    assert not raised.value.node_remains_provisioned
    assert raised.value.cluster_state_ambiguous
    assert raised.value.rollback_error is None


def test_ccm_environment_removes_unsafe_external_overrides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    environment = CcmProvisioner.configure_ccm_environment(
        {
            "PATH": "/bin",
            "SCYLLA_EXT_ENV": "bad",
            "SCYLLA_EXT_OPTS": "bad",
            "SCYLLA_MANAGER_PACKAGE": "bad",
        },
        tmp_path,
    )

    assert environment["PATH"] == "/bin"
    assert environment["SCYLLA_CCM_RUN_DIR"] == str(tmp_path)
    assert "SCYLLA_EXT_ENV" not in environment
    assert "SCYLLA_EXT_OPTS" not in environment
    assert "SCYLLA_MANAGER_PACKAGE" not in environment


@pytest.mark.parametrize(
    ("machine", "architecture"),
    [
        ("aarch64", "aarch64"),
        ("arm64", "aarch64"),
        ("AMD64", "x86_64"),
        ("x86_64", "x86_64"),
    ],
)
def test_ccm_environment_selects_host_package_architecture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    machine: str,
    architecture: str,
) -> None:
    monkeypatch.setattr(platform, "machine", lambda: machine)

    environment = CcmProvisioner.configure_ccm_environment({}, tmp_path)

    assert environment["SCYLLA_ARCH"] == architecture


def test_ccm_environment_replaces_external_package_architecture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(platform, "machine", lambda: "aarch64")

    environment = CcmProvisioner.configure_ccm_environment(
        {"SCYLLA_ARCH": "x86_64"}, tmp_path
    )

    assert environment["SCYLLA_ARCH"] == "aarch64"


def test_ccm_environment_rejects_unsupported_host_architecture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(platform, "machine", lambda: "riscv64")

    with pytest.raises(OSError, match="does not support architecture 'riscv64'"):
        CcmProvisioner.configure_ccm_environment({}, tmp_path)


def prepare_process_cluster(
    provisioner: CcmProvisioner,
) -> tuple[Path, Path, PhysicalTestCluster]:
    """Write safe one-node metadata for PID and removal tests."""
    ccm_directory = make_ccm_directory(provisioner)
    (ccm_directory / "CURRENT").write_text("cluster\n", encoding="ascii")
    cluster_directory = ccm_directory / "cluster"
    node_directory = cluster_directory / "node1"
    node_directory.mkdir(parents=True)
    (cluster_directory / "cluster.conf").write_text(
        yaml.safe_dump({"name": "cluster", "nodes": ["node1"], "seeds": ["node1"]}),
        encoding="utf-8",
    )
    (node_directory / "node.conf").write_text(
        yaml.safe_dump({"name": "node1"}), encoding="utf-8"
    )
    cluster = PhysicalTestCluster(
        provisioner,
        "cluster",
        8,
        ccm_directory,
        ClusterSpec().with_topology(ClusterTopology.single_datacenter(1)),
        [TestClusterNode("node1", "127.0.8.1", "dc1", "RAC1")],
        None,
        None,
    )
    return ccm_directory, node_directory, cluster


@pytest.mark.parametrize("command_fails", [False, True])
def test_orphaned_node_removal_requires_next_run_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command_fails: bool,
) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory, node_directory, _cluster = prepare_process_cluster(provisioner)

    def partially_remove_node(directory: Path, arguments: Sequence[str]) -> None:
        provisioner.ccm_commands.append(tuple(arguments))
        (directory / "cluster/cluster.conf").write_text(
            yaml.safe_dump({"name": "cluster", "nodes": [], "seeds": []}),
            encoding="utf-8",
        )
        if command_fails:
            raise CcmCommandError(arguments, 19, "injected partial removal")

    monkeypatch.setattr(provisioner, "_run_ccm", partially_remove_node)

    with pytest.raises(
        CcmProcessCleanupError, match="removed from membership"
    ) as raised:
        provisioner._remove_node_by_name(  # noqa: SLF001 -- recovery contract
            ccm_directory, "node1"
        )

    assert requires_next_run_recovery(raised.value)
    assert node_directory.is_dir()
    assert (
        yaml.safe_load(
            (ccm_directory / "cluster/cluster.conf").read_text(encoding="utf-8")
        )["nodes"]
        == []
    )
    assert isinstance(raised.value.__cause__, CcmCommandError) is command_fails


def test_interrupted_node_removal_classifies_orphan_for_next_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory, node_directory, _cluster = prepare_process_cluster(provisioner)

    def interrupt_after_membership_update(
        directory: Path, arguments: Sequence[str]
    ) -> None:
        (directory / "cluster/cluster.conf").write_text(
            yaml.safe_dump({"name": "cluster", "nodes": [], "seeds": []}),
            encoding="utf-8",
        )
        raise KeyboardInterrupt

    monkeypatch.setattr(provisioner, "_run_ccm", interrupt_after_membership_update)

    with pytest.raises(
        CcmProcessCleanupError, match="removed from membership"
    ) as raised:
        provisioner._remove_node_by_name(  # noqa: SLF001 -- recovery contract
            ccm_directory, "node1"
        )

    assert isinstance(raised.value.__cause__, KeyboardInterrupt)
    assert requires_next_run_recovery(raised.value)
    assert node_directory.is_dir()


def test_completed_node_removal_preserves_keyboard_interrupt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory, node_directory, _cluster = prepare_process_cluster(provisioner)

    def interrupt_after_removal(directory: Path, arguments: Sequence[str]) -> None:
        (directory / "cluster/cluster.conf").write_text(
            yaml.safe_dump({"name": "cluster", "nodes": [], "seeds": []}),
            encoding="utf-8",
        )
        (node_directory / "node.conf").unlink()
        node_directory.rmdir()
        raise KeyboardInterrupt

    monkeypatch.setattr(provisioner, "_run_ccm", interrupt_after_removal)

    with pytest.raises(KeyboardInterrupt):
        provisioner._remove_node_by_name(  # noqa: SLF001 -- interruption contract
            ccm_directory, "node1"
        )

    assert not node_directory.exists()


def test_cluster_removal_restores_orphaned_node_for_ccm_teardown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory, _node_directory, _cluster = prepare_process_cluster(provisioner)
    cluster_directory = ccm_directory / "cluster"
    orphan = cluster_directory / "node2"
    orphan.mkdir()
    (orphan / "node.conf").write_text(
        yaml.safe_dump({"name": "node2"}), encoding="utf-8"
    )
    inspected: list[str] = []

    def inspect_node(directory: Path, node_name: str) -> bool:
        assert directory == cluster_directory
        inspected.append(node_name)
        return False

    def remove_cluster(directory: Path, arguments: Sequence[str]) -> None:
        assert directory == ccm_directory
        assert arguments[0] == "remove"
        cluster = yaml.safe_load(
            (cluster_directory / "cluster.conf").read_text(encoding="utf-8")
        )
        assert cluster["nodes"] == ["node1", "node2"]
        assert cluster["seeds"] == ["node1"]
        (cluster_directory / "cluster.conf").unlink()

    monkeypatch.setattr(provisioner, "_prepare_node_process_references", inspect_node)
    monkeypatch.setattr(provisioner, "_run_ccm", remove_cluster)

    provisioner._remove_by_name(  # noqa: SLF001 -- recovery contract
        "cluster", ccm_directory
    )

    assert inspected == ["node1", "node2"]
    assert not cluster_directory.exists()


def test_stale_sanitization_clears_unlisted_node_pid_references(
    tmp_path: Path,
) -> None:
    """PID reuse cannot quarantine a node orphaned by partial CCM removal."""
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory, _node_directory, _cluster = prepare_process_cluster(provisioner)
    cluster_directory = ccm_directory / "cluster"
    orphan = cluster_directory / "node2"
    orphan.mkdir()
    (orphan / "node.conf").write_text(
        yaml.safe_dump({"name": "node2", "pid": os.getpid()}), encoding="utf-8"
    )
    (orphan / "cassandra.pid").write_text(f"{os.getpid()}\n", encoding="ascii")

    provisioner._sanitize_stale_process_references(  # noqa: SLF001 -- stale recovery contract
        cluster_directory
    )

    assert "pid" not in yaml.safe_load(
        (orphan / "node.conf").read_text(encoding="utf-8")
    )
    assert not (orphan / "cassandra.pid").exists()


def test_cluster_removal_retains_unrestorable_orphan_with_owned_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory, _node_directory, _cluster = prepare_process_cluster(provisioner)
    cluster_directory = ccm_directory / "cluster"
    orphan = cluster_directory / "node2"
    orphan.mkdir()

    monkeypatch.setattr(
        provisioner,
        "_prepare_node_process_references",
        lambda _directory, node_name: node_name == "node2",
    )

    with pytest.raises(CcmProcessCleanupError, match="without node.conf"):
        provisioner._remove_by_name(  # noqa: SLF001 -- recovery contract
            "cluster", ccm_directory
        )

    assert orphan.is_dir()
    assert provisioner.ccm_commands == []


def test_completed_cluster_removal_preserves_keyboard_interrupt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory, _node_directory, _cluster = prepare_process_cluster(provisioner)
    cluster_directory = ccm_directory / "cluster"

    def interrupt_after_removal(directory: Path, arguments: Sequence[str]) -> None:
        assert directory == ccm_directory
        (cluster_directory / "cluster.conf").unlink()
        raise KeyboardInterrupt

    monkeypatch.setattr(provisioner, "_run_ccm", interrupt_after_removal)

    with pytest.raises(KeyboardInterrupt):
        provisioner._remove_by_name(  # noqa: SLF001 -- interruption contract
            "cluster", ccm_directory
        )

    assert not cluster_directory.exists()


def test_foreign_live_pid_is_rejected_before_ccm_stop(tmp_path: Path) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    _ccm_directory, node_directory, cluster = prepare_process_cluster(provisioner)
    (node_directory / "node.conf").write_text(
        yaml.safe_dump({"name": "node1", "pid": os.getpid()}),
        encoding="utf-8",
    )
    (node_directory / "cassandra.pid").write_text(f"{os.getpid()}\n", encoding="ascii")

    with pytest.raises(CcmProcessCleanupError, match="unrelated live PID"):
        provisioner.stop(cluster)
    assert provisioner.ccm_commands == []


def test_dead_pid_references_are_sanitized_before_ccm_use(tmp_path: Path) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory, node_directory, _cluster = prepare_process_cluster(provisioner)
    missing_pid = 999_999_999
    assert not (Path("/proc") / str(missing_pid)).exists()
    (node_directory / "node.conf").write_text(
        yaml.safe_dump({"name": "node1", "pid": missing_pid}),
        encoding="utf-8",
    )
    for filename in ("cassandra.pid", "scylla-jmx.pid", "scylla-agent.pid"):
        (node_directory / filename).write_text(f"{missing_pid}\n", encoding="ascii")

    remains = provisioner._prepare_node_process_references(  # noqa: SLF001 -- PID safety contract
        ccm_directory / "cluster", "node1"
    )

    assert not remains
    assert "pid" not in yaml.safe_load(
        (node_directory / "node.conf").read_text(encoding="utf-8")
    )
    assert not (node_directory / "cassandra.pid").exists()
    assert not (node_directory / "scylla-jmx.pid").exists()
    assert not (node_directory / "scylla-agent.pid").exists()


@pytest.mark.parametrize(
    "mutation",
    [
        {"name": "other", "nodes": ["node1"], "seeds": ["node1"]},
        {"name": "cluster", "nodes": ["node1", "node1"], "seeds": ["node1"]},
        {"name": "cluster", "nodes": ["node1"], "seeds": ["node2"]},
    ],
)
def test_unsafe_cluster_metadata_blocks_ccm_commands(
    tmp_path: Path, mutation: dict[str, object]
) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory, _node_directory, _cluster = prepare_process_cluster(provisioner)
    (ccm_directory / "cluster/cluster.conf").write_text(
        yaml.safe_dump(mutation), encoding="utf-8"
    )

    with pytest.raises(OSError):
        CcmProvisioner._run_ccm(  # noqa: SLF001 -- metadata safety contract
            provisioner, ccm_directory, ["stop", "--config-dir", str(ccm_directory)]
        )
    assert provisioner.ccm_commands == []


def test_dangling_cluster_metadata_is_never_treated_as_absent(
    tmp_path: Path,
) -> None:
    provisioner = RecordingProvisioner(tmp_path)
    ccm_directory, _node_directory, _cluster = prepare_process_cluster(provisioner)
    cluster_config = ccm_directory / "cluster/cluster.conf"
    cluster_config.unlink()
    cluster_config.symlink_to(tmp_path / "missing-cluster.conf")

    with pytest.raises(OSError, match="non-regular"):
        provisioner._remove_by_name(  # noqa: SLF001 -- path safety contract
            "cluster", ccm_directory
        )
    assert (ccm_directory / "cluster").is_dir()
    assert provisioner.ccm_commands == []


def test_process_group_inspection_failure_requires_next_run_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provisioner = make_provisioner(tmp_path)

    class FakeProcess:
        pid = 123_456_789

        def poll(self) -> int | None:
            return None

        def wait(self, timeout: float) -> int:
            return 0

    def ignore_signal(process_group: int, signal_number: int) -> None:
        return

    def fail_inspection(process_group: int, timeout: float) -> None:
        raise OSError("cannot inspect process group")

    monkeypatch.setattr(os, "killpg", ignore_signal)
    monkeypatch.setattr(provisioner, "_wait_for_process_group", fail_inspection)

    with pytest.raises(CcmProcessCleanupError) as raised:
        provisioner._terminate_process_group(  # noqa: SLF001 -- process safety contract
            cast("subprocess.Popen[bytes]", FakeProcess())
        )
    assert raised.value.recovery_required


class InterruptedProvisioner(RecordingProvisioner):
    """Create interruption plus failed rollback exposes durable cluster state."""

    def _run_ccm(self, ccm_directory: Path, arguments: Sequence[str]) -> None:
        self.ccm_commands.append(tuple(arguments))
        if arguments[0] == "create":
            (ccm_directory / "CURRENT").write_text("interrupted\n", encoding="ascii")
            cluster_directory = ccm_directory / "interrupted"
            cluster_directory.mkdir()
            (cluster_directory / "cluster.conf").write_text(
                yaml.safe_dump({"name": "interrupted", "nodes": [], "seeds": []}),
                encoding="utf-8",
            )
            raise KeyboardInterrupt
        if arguments[0] == "remove":
            raise OSError("injected rollback failure")


def test_keyboard_interrupt_runs_rollback_and_retains_ambiguous_cluster(
    tmp_path: Path,
) -> None:
    """SIGINT-like BaseException cannot release partially provisioned state."""
    provisioner = InterruptedProvisioner(tmp_path)
    spec = (
        ClusterSpec()
        .with_topology(ClusterTopology.single_datacenter(1))
        .with_transports(AlternatorTransport.HTTP)
    )

    with pytest.raises(CcmClusterProvisioningError) as raised:
        provisioner.provision(spec, "interrupted", 17)

    assert isinstance(raised.value.provisioning_error, KeyboardInterrupt)
    assert str(raised.value.rollback_error) == "injected rollback failure"
    assert raised.value.cluster.instance_id == "interrupted"
    assert raised.value.cluster.is_dirty is False
    assert [command[0] for command in provisioner.ccm_commands] == [
        "create",
        "remove",
    ]
