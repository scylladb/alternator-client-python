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

"""Real-cluster conformance tests for native CCM test infrastructure."""

from __future__ import annotations

import contextlib
import json
import os
import signal
import socket
import ssl
import subprocess
import sys
import textwrap
import time
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import pytest
import yaml
from botocore.exceptions import ClientError

from alternator import Auth, close_client, create_client
from tests.integration import integration_tests_enabled
from tests.testinfra.ccm_provisioner import (
    COMMAND_TIMEOUT,
    PROCESS_KILL_TIMEOUT,
    PROCESS_TERMINATION_GRACE,
    READINESS_TIMEOUT,
    CcmProvisioner,
)
from tests.testinfra.cluster import HTTPS_PORT, TestClusterNode
from tests.testinfra.cluster_spec import (
    AlternatorTransport,
    ClusterSecuritySpec,
    ClusterSpecs,
    ClusterTopology,
)
from tests.testinfra.pool import TestClusterPool, TestClusters
from tests.testinfra.run_state import CcmRunState, Manifest

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not integration_tests_enabled(),
        reason="Integration tests disabled",
    ),
]


def _terminate_process_groups(process_groups: set[int]) -> None:
    """Bound termination of test-owned Linux process groups."""
    remaining = {
        process_group
        for process_group in process_groups
        if process_group > 1 and process_group != os.getpgrp()
    }
    for signal_number, timeout in (
        (signal.SIGTERM, PROCESS_TERMINATION_GRACE),
        (signal.SIGKILL, PROCESS_KILL_TIMEOUT),
    ):
        for process_group in remaining:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process_group, signal_number)
        deadline = time.monotonic() + timeout
        while remaining and time.monotonic() < deadline:
            remaining = {
                process_group
                for process_group in remaining
                if CcmProvisioner._process_group_has_live_members(  # noqa: SLF001 -- integration cleanup
                    process_group
                )
            }
            if remaining:
                time.sleep(0.02)
        if not remaining:
            return
    raise RuntimeError(f"Test-owned process groups survived cleanup: {remaining}")


def _marked_ccm_process_groups(root: Path) -> set[int]:
    """Find detached CCM descendants belonging to this test's private root."""
    runs_directory = (root / "runs").absolute()
    process_groups: set[int] = set()
    for process_directory in Path("/proc").iterdir():
        if not process_directory.name.isascii() or not process_directory.name.isdigit():
            continue
        try:
            environment = (process_directory / "environ").read_bytes().split(b"\0")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        marker = next(
            (
                value.removeprefix(b"SCYLLA_CCM_RUN_DIR=")
                for value in environment
                if value.startswith(b"SCYLLA_CCM_RUN_DIR=")
            ),
            None,
        )
        if marker is None:
            continue
        try:
            run_directory = Path(marker.decode("utf-8")).absolute()
            process_group = os.getpgid(int(process_directory.name))
        except (OSError, UnicodeError, ValueError):
            continue
        if run_directory.parent == runs_directory:
            process_groups.add(process_group)
    return process_groups


def _reap_test_child(child: subprocess.Popen[str] | None) -> None:
    if child is None:
        return
    if child.poll() is None:
        _terminate_process_groups({child.pid})
    child.communicate(timeout=PROCESS_KILL_TIMEOUT)


def _create_table(client: DynamoDBClient, table_name: str) -> None:
    client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    client.get_waiter("table_exists").wait(TableName=table_name)


def _assert_https_endpoint_works(endpoint: str, ca_certificate: Path) -> None:
    context = ssl.create_default_context(cafile=str(ca_certificate))
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context)
    )
    with opener.open(endpoint, timeout=5) as response:
        assert 200 <= response.status < 300


def _await_endpoint_closed(endpoint: str) -> None:
    parsed = urlsplit(endpoint)
    assert parsed.hostname is not None
    assert parsed.port is not None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((parsed.hostname, parsed.port), timeout=0.5):
                pass
        except OSError:
            return
        time.sleep(0.1)
    raise AssertionError(
        f"Alternator endpoint remained reachable after stop: {endpoint}"
    )


def _https_endpoint(node: TestClusterNode) -> str:
    return f"https://{node.address}:{HTTPS_PORT}"


def test_same_spec_reuses_cluster_with_independent_resource_scopes() -> None:
    """CCM-REQ-003: one table namespace closes without touching another."""
    with (
        TestClusters.acquire_reusable(ClusterSpecs.default_spec()) as first,
        TestClusters.acquire_reusable(ClusterSpecs.default_spec()) as second,
    ):
        assert first.cluster.instance_id == second.cluster.instance_id
        first_table = first.resources.new_table_name("first" * 100)
        second_table = second.resources.new_table_name("second")
        assert len(first_table) <= 192
        client = second.cluster.create_client(AlternatorTransport.HTTP)
        try:
            _create_table(client, first_table)
            _create_table(client, second_table)
            assert (
                client.describe_table(TableName=first_table)["Table"]["TableName"]
                == first_table
            )
            assert (
                client.describe_table(TableName=second_table)["Table"]["TableName"]
                == second_table
            )

            first.close()

            with pytest.raises(client.exceptions.ResourceNotFoundException):
                client.describe_table(TableName=first_table)
            assert (
                client.describe_table(TableName=second_table)["Table"]["TableName"]
                == second_table
            )
        finally:
            close_client(client)


def test_authorized_cluster_provides_working_credentials() -> None:
    """CCM-REQ-002: wrong signing credentials fail and lease credentials work."""
    spec = (
        ClusterSpecs.default_spec()
        .with_topology(ClusterTopology.single_datacenter(1))
        .with_transports(AlternatorTransport.HTTP)
        .with_security(ClusterSecuritySpec.ENFORCED)
    )
    with TestClusters.acquire_reusable(spec) as lease:
        connection = lease.cluster.connection(AlternatorTransport.HTTP)
        unauthorized = create_client(
            connection.client_config(),
            auth=Auth.static_credentials("wrong-user", "wrong-password"),
        )
        authorized = lease.cluster.create_client(AlternatorTransport.HTTP)
        try:
            with pytest.raises(ClientError):
                unauthorized.list_tables()
            assert isinstance(authorized.list_tables()["TableNames"], list)
        finally:
            close_client(unauthorized)
            close_client(authorized)


def test_https_only_reusable_cluster_cleans_its_resource_scope() -> None:
    """CCM-REQ-003: HTTPS fallback deletes lease-owned tables on close."""
    spec = (
        ClusterSpecs.default_spec()
        .with_topology(ClusterTopology.single_datacenter(1))
        .with_transports(AlternatorTransport.HTTPS)
    )
    lease = TestClusters.acquire_reusable(spec)
    table_name = lease.resources.new_table_name("https_cleanup")
    client = lease.cluster.create_client(AlternatorTransport.HTTPS)
    try:
        _create_table(client, table_name)
    finally:
        close_client(client)

    lease.close()

    with TestClusters.acquire_reusable(spec) as reused:
        client = reused.cluster.create_client(AlternatorTransport.HTTPS)
        try:
            with pytest.raises(client.exceptions.ResourceNotFoundException):
                client.describe_table(TableName=table_name)
        finally:
            close_client(client)


def test_private_https_cluster_can_change_lifecycle_and_topology() -> None:
    """CCM-REQ-004: private controls mutate cluster through generated CA."""
    spec = (
        ClusterSpecs.default_spec()
        .with_topology(ClusterTopology.single_datacenter(1))
        .with_transports(AlternatorTransport.HTTPS)
    )
    with TestClusters.provision_private(spec) as lease:
        original = lease.cluster.nodes[0]
        added = lease.control.add_node("dc1", "RAC1")
        assert len(lease.cluster.nodes) == 2
        connection = lease.cluster.connection(AlternatorTransport.HTTPS)
        assert connection.ca_certificate_path is not None
        https_client = lease.cluster.create_client(AlternatorTransport.HTTPS)
        try:
            assert isinstance(https_client.list_tables()["TableNames"], list)
        finally:
            close_client(https_client)
        added_endpoint = _https_endpoint(added)
        _assert_https_endpoint_works(added_endpoint, connection.ca_certificate_path)

        lease.control.stop()
        _await_endpoint_closed(connection.seed_endpoint)
        _await_endpoint_closed(added_endpoint)
        lease.control.start()
        _assert_https_endpoint_works(
            connection.seed_endpoint, connection.ca_certificate_path
        )
        _assert_https_endpoint_works(added_endpoint, connection.ca_certificate_path)

        lease.control.stop_node(added)
        _await_endpoint_closed(added_endpoint)
        lease.control.start_node(added)
        _assert_https_endpoint_works(added_endpoint, connection.ca_certificate_path)
        lease.control.remove_node(original)
        assert len(lease.cluster.nodes) == 1

        replacement = lease.control.add_node("dc1", "RAC1")
        assert replacement.address == original.address
        _assert_https_endpoint_works(
            _https_endpoint(replacement), connection.ca_certificate_path
        )
        lease.control.remove_node(replacement)


def test_yaml_overrides_survive_cluster_and_node_updates() -> None:
    """CCM-REQ-001: typed YAML values persist for initial and added nodes."""
    spec = (
        ClusterSpecs.default_spec()
        .with_topology(ClusterTopology.single_datacenter(1))
        .with_transports(AlternatorTransport.HTTPS)
        .with_yaml_override("hinted_handoff_enabled", "false")
        .with_yaml_override("commitlog_sync", "batch")
        .with_yaml_override("commitlog_sync_batch_window_in_ms", "17")
        .with_yaml_override("commitlog_sync_period_in_ms", "null")
        .with_yaml_override("experimental_features", "null")
    )
    with TestClusters.provision_private(spec) as lease:
        instance_id = lease.cluster.instance_id
        lease.control.add_node("dc1", "RAC1")
        assert len(lease.cluster.nodes) == 2

    diagnostics = Path(os.environ.get("SCYLLA_CCM_DIAGNOSTICS_DIR", "test-results/ccm"))
    for node_name in ("node1", "node2"):
        config_path = (
            diagnostics / instance_id / instance_id / node_name / "conf" / "scylla.yaml"
        )
        parsed = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        assert parsed["hinted_handoff_enabled"] is False
        assert parsed["commitlog_sync"] == "batch"
        assert parsed["commitlog_sync_batch_window_in_ms"] == 17
        assert parsed["commitlog_sync_period_in_ms"] is None
        assert parsed["experimental_features"] is None


def test_hard_killed_process_is_recovered_before_reprovisioning(
    tmp_path: Path,
) -> None:
    """CCM-REQ-006: next process reaps hard-killed ownership before reuse."""
    root = tmp_path / "shared-state"
    diagnostics = tmp_path / "diagnostics"
    environment = dict(os.environ)
    environment["SCYLLA_CCM_ROOT"] = str(root)
    environment["SCYLLA_CCM_DIAGNOSTICS_DIR"] = str(diagnostics)
    child_program = textwrap.dedent(
        """
        import json
        import os

        from tests.testinfra import (
            AlternatorTransport,
            ClusterSpecs,
            ClusterTopology,
            TestClusters,
        )

        spec = (
            ClusterSpecs.default_spec()
            .with_topology(ClusterTopology.single_datacenter(1))
            .with_transports(AlternatorTransport.HTTP)
        )
        lease = TestClusters.provision_private(spec)
        node = lease.cluster.nodes[0]
        print(
            json.dumps(
                {
                    "instance_id": lease.cluster.instance_id,
                    "ccm_id": int(node.address.split(".")[2]),
                }
            ),
            flush=True,
        )
        os._exit(0)
        """
    )

    def cleanup_stale(run_directory: Path, manifest: Manifest) -> None:
        provisioner = CcmProvisioner(run_directory, diagnostics_directory=diagnostics)
        provisioner.cleanup_stale_cluster(
            manifest.instance_id,
            manifest.ccm_id,
            run_directory / "clusters" / manifest.instance_id,
        )

    child: subprocess.Popen[str] | None = None
    state: CcmRunState | None = None
    cleanup_errors: list[BaseException] = []
    try:
        child = subprocess.Popen(
            [sys.executable, "-c", child_program],
            cwd=Path(__file__).resolve().parents[2],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        child_stdout, child_stderr = child.communicate(
            timeout=(
                COMMAND_TIMEOUT
                + READINESS_TIMEOUT
                + PROCESS_TERMINATION_GRACE
                + PROCESS_KILL_TIMEOUT
                + 30.0
            )
        )
        if child.returncode != 0:
            raise subprocess.CalledProcessError(
                child.returncode,
                child.args,
                output=child_stdout,
                stderr=child_stderr,
            )
        payload = json.loads(child_stdout.splitlines()[-1])
        stale_id = int(payload["ccm_id"])
        stale_runs = list((root / "runs").glob("ccm-runtime.*"))
        assert len(stale_runs) == 1
        stale_run = stale_runs[0]

        state = CcmRunState.open(root, cleanup_stale)
        assert not stale_run.exists()
        assert not (root / "ccm-id-locks" / f"{stale_id}.owner").exists()
        command_logs = list(
            (diagnostics / payload["instance_id"]).glob("ccm-command-*.log")
        )
        assert command_logs
        assert any(
            " create " in path.read_text(encoding="utf-8") for path in command_logs
        )
        provisioner = CcmProvisioner(
            state.run_directory, diagnostics_directory=diagnostics
        )
        pool = TestClusterPool(
            provisioner,
            maximum_nodes=1,
            cleanup_resources=lambda _resources: None,
            run_state=state,
        )
        replacement_spec = (
            ClusterSpecs.default_spec()
            .with_topology(ClusterTopology.single_datacenter(1))
            .with_transports(AlternatorTransport.HTTP)
        )
        with pool, pool.provision_private(replacement_spec) as replacement:
            client = replacement.cluster.create_client(AlternatorTransport.HTTP)
            try:
                assert isinstance(client.list_tables()["TableNames"], list)
            finally:
                close_client(client)
    finally:
        failure_in_flight = sys.exc_info()[0] is not None
        try:
            _reap_test_child(child)
        except BaseException as exception:
            cleanup_errors.append(exception)
        try:
            _terminate_process_groups(_marked_ccm_process_groups(root))
        except BaseException as exception:
            cleanup_errors.append(exception)
        if state is not None:
            try:
                state.close()
            except BaseException as exception:
                cleanup_errors.append(exception)
        final_state: CcmRunState | None = None
        try:
            final_state = CcmRunState.open(root, cleanup_stale)
            final_state.close()
        except BaseException as exception:
            cleanup_errors.append(exception)
        if cleanup_errors and not failure_in_flight:
            raise cleanup_errors[0]
