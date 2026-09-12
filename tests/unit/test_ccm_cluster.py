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

"""Unit contracts for CCM cluster views, controls, resources, and leases."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from alternator import Auth, Config
from tests.testinfra.cluster import (
    AlternatorConnection,
    ClusterPool,
    PhysicalTestCluster,
    PrivateClusterLease,
    ReadOnlyTestCluster,
    ReusableClusterLease,
    TestClusterNode,
    TestResourceScope,
)
from tests.testinfra.cluster_spec import AlternatorTransport, ClusterSpec

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient

    from tests.testinfra.ccm_provisioner import CcmProvisioner


class FakeProvisioner:
    """Deterministic lifecycle backend for physical-cluster tests."""

    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self.running: dict[str, bool] = {"node1": True}
        self.fail_add = False

    def is_node_running(
        self, cluster: PhysicalTestCluster, node: TestClusterNode
    ) -> bool:
        self.calls.append(("is_node_running", node.name))
        return self.running.get(node.name, False)

    def wait_for_node_ready(
        self, cluster: PhysicalTestCluster, node: TestClusterNode
    ) -> None:
        self.calls.append(("wait_for_node_ready", node.name))

    def start(
        self, cluster: PhysicalTestCluster, nodes: tuple[TestClusterNode, ...]
    ) -> None:
        self.calls.append(("start", *(node.name for node in nodes)))
        for node in nodes:
            self.running[node.name] = True

    def stop(self, cluster: PhysicalTestCluster) -> None:
        self.calls.append(("stop",))
        for node in cluster.nodes:
            self.running[node.name] = False

    def start_node(self, cluster: PhysicalTestCluster, node: TestClusterNode) -> None:
        self.calls.append(("start_node", node.name))
        self.running[node.name] = True

    def stop_node(self, cluster: PhysicalTestCluster, node: TestClusterNode) -> None:
        self.calls.append(("stop_node", node.name))
        self.running[node.name] = False

    def add_node(
        self, cluster: PhysicalTestCluster, datacenter: str, rack: str
    ) -> TestClusterNode:
        self.calls.append(("add_node", datacenter, rack))
        if self.fail_add:
            raise OSError("ambiguous add")
        node = TestClusterNode("node2", "127.0.7.2", datacenter, rack)
        self.running[node.name] = True
        return node

    def decommission_node(
        self, cluster: PhysicalTestCluster, node: TestClusterNode
    ) -> None:
        self.calls.append(("decommission_node", node.name))

    def delete_node_state(
        self, cluster: PhysicalTestCluster, node: TestClusterNode
    ) -> None:
        self.calls.append(("delete_node_state", node.name))

    def remove(self, cluster: PhysicalTestCluster) -> None:
        self.calls.append(("remove", cluster.instance_id))


class FakePool:
    """Record lease and private capacity callbacks."""

    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def release_reusable(self, token: object, resources: TestResourceScope) -> None:
        self.calls.append(("release_reusable", token, resources))

    def release_private(self, cluster: PhysicalTestCluster) -> None:
        self.calls.append(("release_private", cluster))

    def reserve_additional_private_node(self, cluster: PhysicalTestCluster) -> None:
        self.calls.append(("reserve", cluster))

    def release_additional_private_node(self, cluster: PhysicalTestCluster) -> None:
        self.calls.append(("release_node", cluster))


def make_cluster(
    tmp_path: Path,
    *,
    provisioner: FakeProvisioner | None = None,
    spec: ClusterSpec | None = None,
    ca_certificate_path: Path | None = None,
    credentials: Auth | None = None,
) -> PhysicalTestCluster:
    """Build physical cluster without running external commands."""
    backend = provisioner or FakeProvisioner()
    return PhysicalTestCluster(
        cast("CcmProvisioner", backend),
        "python-test",
        7,
        tmp_path / "ccm",
        spec or ClusterSpec(),
        [TestClusterNode("node1", "127.0.7.1", "dc1", "RAC1")],
        ca_certificate_path,
        credentials,
    )


def test_connection_exposes_all_endpoints_credentials_and_strict_ca(
    tmp_path: Path,
) -> None:
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text("test CA", encoding="ascii")
    credentials = Auth.static_credentials("user", "secret")
    cluster = make_cluster(
        tmp_path, ca_certificate_path=ca_path, credentials=credentials
    )

    connection = cluster.connection(AlternatorTransport.HTTPS)
    config = connection.client_config()

    assert connection.seed_endpoint == "https://127.0.7.1:8043"
    assert connection.node_endpoints == ("https://127.0.7.1:8043",)
    assert connection.credentials is credentials
    assert connection.ca_certificate_path == ca_path
    assert config.seed_hosts == ["127.0.7.1"]
    assert config.port == 8043
    assert config.scheme == "https"
    assert config.tls.custom_ca_cert_paths == (ca_path,)
    assert not config.tls.trust_system_ca_certs


def test_https_connection_passes_generated_ca_to_sdk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Generated CA configures discovery and botocore verification."""
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text("test CA", encoding="ascii")
    connection = AlternatorConnection(
        "https://127.0.7.1:8043",
        ("https://127.0.7.1:8043",),
        ca_certificate_path=ca_path,
    )
    captured: dict[str, object] = {}

    def fake_create_client(
        config: object, *, auth: Auth | None, **kwargs: object
    ) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("tests.testinfra.cluster._create_client", fake_create_client)

    connection.create_client()

    assert captured["verify"] == str(ca_path)


def test_connection_rejects_transport_not_in_spec(tmp_path: Path) -> None:
    spec = ClusterSpec().with_transports(AlternatorTransport.HTTP)
    cluster = make_cluster(tmp_path, spec=spec)

    with pytest.raises(RuntimeError, match="does not provide HTTPS"):
        cluster.connection(AlternatorTransport.HTTPS)


def test_cluster_connections_and_added_nodes_bypass_environment_proxies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NO_PROXY", "localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    cluster = make_cluster(tmp_path)

    cluster.connection(AlternatorTransport.HTTP)
    added = cluster.add_node("dc1", "RAC2")

    assert added.address == "127.0.7.2"
    for variable in ("NO_PROXY", "no_proxy"):
        entries = os.environ[variable].split(",")
        assert "127.0.7.1" in entries
        assert "127.0.7.2" in entries


def test_read_only_view_tracks_nodes_and_has_no_mutation(tmp_path: Path) -> None:
    cluster = make_cluster(tmp_path)
    view = ReadOnlyTestCluster(cluster)

    assert view.nodes == cluster.nodes
    assert not hasattr(view, "start_node")
    assert not hasattr(view, "add_node")

    added = cluster.add_node("dc1", "RAC2")
    assert added in view.nodes
    cluster.remove_node(added)
    assert added not in view.nodes


def test_private_lifecycle_serializes_node_add_remove_and_blocks_final_node(
    tmp_path: Path,
) -> None:
    backend = FakeProvisioner()
    pool = FakePool()
    cluster = make_cluster(tmp_path, provisioner=backend)
    resources = TestResourceScope(cluster, "run", 1)
    lease = PrivateClusterLease(cast(ClusterPool, pool), cluster, resources)

    assert not hasattr(lease.cluster, "add_node")
    added = lease.control.add_node("dc1", "RAC2")
    assert added in lease.cluster.nodes

    lease.control.remove_node(added)
    assert added not in lease.cluster.nodes
    assert len(cluster.nodes) == 1
    assert [call[0] for call in pool.calls] == ["reserve", "release_node"]
    with pytest.raises(RuntimeError, match="final node"):
        lease.control.remove_node(cluster.nodes[0])


def test_ambiguous_mutation_dirties_cluster_but_whole_removal_remains_allowed(
    tmp_path: Path,
) -> None:
    backend = FakeProvisioner()
    backend.fail_add = True
    cluster = make_cluster(tmp_path, provisioner=backend)

    with pytest.raises(OSError, match="ambiguous add"):
        cluster.add_node("dc1", "RAC2")
    assert cluster.is_dirty
    with pytest.raises(RuntimeError, match="ambiguous state"):
        cluster.stop()

    cluster.remove_physical()
    cluster.remove_physical()
    assert backend.calls.count(("remove", "python-test")) == 1


def test_keyboard_interrupt_dirties_mutating_cluster(tmp_path: Path) -> None:
    """Ctrl-C during topology change cannot leave cluster reusable."""

    class InterruptingProvisioner(FakeProvisioner):
        def add_node(
            self, cluster: PhysicalTestCluster, datacenter: str, rack: str
        ) -> TestClusterNode:
            raise KeyboardInterrupt

    backend = InterruptingProvisioner()
    cluster = make_cluster(tmp_path, provisioner=backend)

    with pytest.raises(KeyboardInterrupt):
        cluster.add_node("dc1", "RAC2")

    assert cluster.is_dirty
    with pytest.raises(RuntimeError, match="ambiguous state"):
        cluster.start()


def test_add_node_without_running_seed_fails_without_dirtying_cluster(
    tmp_path: Path,
) -> None:
    backend = FakeProvisioner()
    cluster = make_cluster(tmp_path, provisioner=backend)
    cluster.stop()

    with pytest.raises(RuntimeError, match="existing running seed"):
        cluster.add_node("dc1", "RAC2")

    assert not cluster.is_dirty
    assert not any(call[0] == "add_node" for call in backend.calls)


def test_leases_are_context_managers_and_release_once(tmp_path: Path) -> None:
    pool = FakePool()
    cluster = make_cluster(tmp_path)
    resources = TestResourceScope(cluster, "run", 2)
    reusable = ReusableClusterLease(
        cast(ClusterPool, pool), "token", cluster, resources
    )
    private = PrivateClusterLease(cast(ClusterPool, pool), cluster, resources)

    reusable.close()
    reusable.close()
    private.close()
    private.close()

    assert [call[0] for call in pool.calls] == [
        "release_reusable",
        "release_private",
    ]


def test_reusable_lease_close_retries_interrupted_pool_release(tmp_path: Path) -> None:
    class InterruptingPool(FakePool):
        def release_reusable(self, token: object, resources: TestResourceScope) -> None:
            super().release_reusable(token, resources)
            if len(self.calls) == 1:
                raise KeyboardInterrupt

    pool = InterruptingPool()
    cluster = make_cluster(tmp_path)
    resources = TestResourceScope(cluster, "run", 3)
    lease = ReusableClusterLease(cast(ClusterPool, pool), "token", cluster, resources)

    with pytest.raises(KeyboardInterrupt):
        lease.close()
    lease.close()
    lease.close()

    assert [call[0] for call in pool.calls] == [
        "release_reusable",
        "release_reusable",
    ]


class ResourceNotFoundException(Exception):
    """Fake DynamoDB missing-table error."""


class FakeDynamoClient:
    """Small table API used to prove bounded prefix cleanup."""

    class exceptions:
        ResourceNotFoundException = ResourceNotFoundException

    def __init__(self) -> None:
        self.deleted: list[str] = []
        self.pages = 0

    def list_tables(self, **kwargs: object) -> dict[str, object]:
        self.pages += 1
        if not kwargs:
            return {
                "TableNames": ["owned_one", "foreign"],
                "LastEvaluatedTableName": "foreign",
            }
        return {"TableNames": ["owned_two"]}

    def delete_table(self, *, TableName: str) -> None:  # noqa: N803 -- boto API
        self.deleted.append(TableName)

    def describe_table(self, *, TableName: str) -> None:  # noqa: N803 -- boto API
        raise ResourceNotFoundException(TableName)


def test_resource_scope_sanitizes_names_and_cleans_only_owned_prefix() -> None:
    long_hint = "Unicode/雪!" * 100
    connection = AlternatorConnection("http://127.0.0.1:8080", ())
    assert connection.client_config().scheme == "http"

    scope_name = TestResourceScope._sanitize(long_hint)  # noqa: SLF001 -- contract test
    assert set(scope_name) <= set("abcdefghijklmnopqrstuvwxyz0123456789_-.")

    client = FakeDynamoClient()
    TestResourceScope.cleanup_tables(
        cast("DynamoDBClient", client), "owned_", timeout=1.0
    )
    assert client.pages == 2
    assert client.deleted == ["owned_one", "owned_two"]


def test_resource_scope_limits_long_names_for_scylla(tmp_path: Path) -> None:
    """Generated names fit Alternator's stricter 192-character limit."""
    scope = TestResourceScope(make_cluster(tmp_path), "run", 1)

    table_name = scope.new_table_name("long-hint" * 100)

    assert len(table_name) <= 192


def test_https_only_scope_cleanup_uses_ca_and_single_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HTTPS cleanup uses generated CA with bounded SDK retry behavior."""
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text("test CA", encoding="ascii")
    spec = ClusterSpec().with_transports(AlternatorTransport.HTTPS)
    cluster = make_cluster(tmp_path, spec=spec, ca_certificate_path=ca_path)
    scope = TestResourceScope(cluster, "run", 3)
    fake_client = object()
    captured: dict[str, object] = {}

    def fake_create_client(
        config: Config, *, auth: Auth | None, **kwargs: object
    ) -> object:
        captured["config"] = config
        captured.update(kwargs)
        return fake_client

    def fake_cleanup_tables(client: object, prefix: str, timeout: float) -> None:
        assert client is fake_client
        assert prefix == scope.prefix
        assert timeout == 120.0

    monkeypatch.setattr("tests.testinfra.cluster._create_client", fake_create_client)
    monkeypatch.setattr("tests.testinfra.cluster.close_client", lambda _client: None)
    monkeypatch.setattr(
        TestResourceScope, "cleanup_tables", staticmethod(fake_cleanup_tables)
    )

    scope.cleanup()

    config = cast(Config, captured["config"])
    assert captured["verify"] == str(ca_path)
    assert config.retries.max_attempts == 1
    assert config.timeouts.read_seconds == 10.0
