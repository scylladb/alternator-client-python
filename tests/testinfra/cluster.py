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

"""Cluster views and leases used by native CCM integration tests."""

from __future__ import annotations

import os
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, Protocol
from urllib.parse import urlsplit

from alternator import (
    TLS,
    Auth,
    Config,
    RetryConfig,
    TimeoutConfig,
    close_client,
)
from alternator import (
    create_client as _create_client,
)
from tests.testinfra.cluster_spec import AlternatorTransport, ClusterSpec

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from mypy_boto3_dynamodb import DynamoDBClient

    from tests.testinfra.ccm_provisioner import CcmProvisioner


HTTP_PORT = 8080
HTTPS_PORT = 8043
MAXIMUM_TABLE_NAME_LENGTH = 192
_UNIQUE_SUFFIX_LENGTH = 33
_DEFAULT_RESOURCE_CLEANUP_TIMEOUT = 120.0
_MAXIMUM_API_CALL_TIMEOUT = 10.0
_POLL_INTERVAL = 0.1


def add_no_proxy_hosts(hosts: Iterable[str]) -> None:
    """Keep local CCM nodes out of environment-configured proxies."""
    node_hosts = tuple(hosts)
    entries: list[str] = []
    for variable in ("NO_PROXY", "no_proxy"):
        for entry in os.environ.get(variable, "").split(","):
            normalized = entry.strip()
            if normalized and normalized not in entries:
                entries.append(normalized)
    entries.extend(host for host in node_hosts if host not in entries)
    value = ",".join(entries)
    for variable in ("NO_PROXY", "no_proxy"):
        os.environ[variable] = value


class ClusterPool(Protocol):
    """Operations leases and private controls require from their owning pool."""

    def release_reusable(self, token: object, resources: TestResourceScope) -> None: ...

    def release_private(self, cluster: PhysicalTestCluster) -> None: ...

    def reserve_additional_private_node(self, cluster: PhysicalTestCluster) -> None: ...

    def release_additional_private_node(self, cluster: PhysicalTestCluster) -> None: ...


@dataclass(frozen=True, slots=True)
class TestClusterNode:
    """Node provisioned by CCM."""

    __test__ = False

    name: str
    address: str
    datacenter: str
    rack: str


@dataclass(frozen=True, slots=True)
class AlternatorConnection:
    """Connection material for one Alternator transport."""

    seed_endpoint: str
    node_endpoints: tuple[str, ...]
    credentials: Auth | None = None
    ca_certificate_path: Path | None = None

    def client_config(self) -> Config:
        """Build client configuration with strict generated-CA trust for HTTPS."""
        endpoint = urlsplit(self.seed_endpoint)
        if endpoint.hostname is None or endpoint.port is None:
            raise ValueError(f"Invalid Alternator endpoint: {self.seed_endpoint}")
        raw_scheme = endpoint.scheme
        if raw_scheme not in ("http", "https"):
            raise ValueError(f"Unsupported Alternator endpoint scheme: {raw_scheme}")
        scheme: Literal["http", "https"] = "https" if raw_scheme == "https" else "http"
        tls = TLS.system_default()
        if scheme == "https" and self.ca_certificate_path is not None:
            tls = TLS(
                custom_ca_cert_paths=(self.ca_certificate_path,),
                trust_system_ca_certs=False,
            )
        return Config(
            seed_hosts=[endpoint.hostname],
            port=endpoint.port,
            scheme=scheme,
            tls=tls,
        )

    def create_client(
        self,
        *,
        config: Config | None = None,
        **boto_kwargs: Any,  # noqa: ANN401 -- boto kwargs are dynamic
    ) -> DynamoDBClient:
        """Create load-balanced client using connection endpoint and credentials."""
        if self.ca_certificate_path is not None:
            boto_kwargs.setdefault("verify", str(self.ca_certificate_path))
        return _create_client(
            config or self.client_config(), auth=self.credentials, **boto_kwargs
        )


class TestClusterInfo(Protocol):
    """Read-only information available to every lease."""

    @property
    def instance_id(self) -> str: ...

    @property
    def spec(self) -> ClusterSpec: ...

    @property
    def nodes(self) -> tuple[TestClusterNode, ...]: ...

    def connection(self, transport: AlternatorTransport) -> AlternatorConnection: ...

    def client_config(self, transport: AlternatorTransport) -> Config: ...

    def create_client(
        self,
        transport: AlternatorTransport,
        **boto_kwargs: Any,  # noqa: ANN401 -- boto kwargs are dynamic
    ) -> DynamoDBClient: ...


class _LifecycleState(Enum):
    OPEN = auto()
    REMOVING = auto()
    REMOVAL_FAILED = auto()
    CLOSED = auto()


class _NodeState(Enum):
    RUNNING = auto()
    STOPPED = auto()
    DECOMMISSIONED = auto()


class PhysicalTestCluster:
    """Mutable physical cluster hidden behind read-only or private lease views."""

    def __init__(
        self,
        provisioner: CcmProvisioner,
        instance_id: str,
        ccm_id: int,
        ccm_directory: Path,
        spec: ClusterSpec,
        nodes: Sequence[TestClusterNode],
        ca_certificate_path: Path | None,
        credentials: Auth | None,
    ) -> None:
        self._provisioner = provisioner
        self._instance_id = instance_id
        self._ccm_id = ccm_id
        self._ccm_directory = ccm_directory
        self._spec = spec
        self._nodes = list(nodes)
        self._node_states = {id(node): _NodeState.RUNNING for node in nodes}
        self._ca_certificate_path = ca_certificate_path
        self._credentials = credentials
        self._lifecycle_state = _LifecycleState.OPEN
        self._dirty = False
        self._recovery_required = False
        self._lock = threading.RLock()

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def ccm_id(self) -> int:
        return self._ccm_id

    @property
    def ccm_directory(self) -> Path:
        return self._ccm_directory

    @property
    def spec(self) -> ClusterSpec:
        return self._spec

    @property
    def nodes(self) -> tuple[TestClusterNode, ...]:
        with self._lock:
            return tuple(self._nodes)

    @property
    def ca_certificate_path(self) -> Path | None:
        return self._ca_certificate_path

    @property
    def credentials(self) -> Auth | None:
        return self._credentials

    @property
    def is_dirty(self) -> bool:
        with self._lock:
            return self._dirty

    def connection(self, transport: AlternatorTransport) -> AlternatorConnection:
        with self._lock:
            if transport not in self._spec.transports:
                raise RuntimeError(
                    f"Cluster {self._instance_id!r} does not provide {transport.name}"
                )
            scheme = "http" if transport is AlternatorTransport.HTTP else "https"
            port = HTTP_PORT if transport is AlternatorTransport.HTTP else HTTPS_PORT
            endpoints = tuple(
                f"{scheme}://{node.address}:{port}" for node in self._nodes
            )
            if not endpoints:
                raise RuntimeError(f"Cluster {self._instance_id!r} has no nodes")
            add_no_proxy_hosts(node.address for node in self._nodes)
            return AlternatorConnection(
                seed_endpoint=endpoints[0],
                node_endpoints=endpoints,
                credentials=self._credentials,
                ca_certificate_path=(
                    self._ca_certificate_path
                    if transport is AlternatorTransport.HTTPS
                    else None
                ),
            )

    def client_config(self, transport: AlternatorTransport) -> Config:
        return self.connection(transport).client_config()

    def create_client(
        self,
        transport: AlternatorTransport,
        **boto_kwargs: Any,  # noqa: ANN401 -- boto kwargs are dynamic
    ) -> DynamoDBClient:
        return self.connection(transport).create_client(**boto_kwargs)

    def start(self) -> None:
        with self._lock:
            self._ensure_mutable()
            stopped: list[TestClusterNode] = []
            for node in self._nodes:
                if self._state(node) is _NodeState.DECOMMISSIONED:
                    continue
                if self._probe_node_running(node):
                    self._provisioner.wait_for_node_ready(self, node)
                    self._set_state(node, _NodeState.RUNNING)
                else:
                    self._set_state(node, _NodeState.STOPPED)
                    stopped.append(node)
            if not stopped:
                return
            try:
                self._provisioner.start(self, stopped)
                for node in stopped:
                    self._set_state(node, _NodeState.RUNNING)
            except BaseException as exception:
                self._record_ambiguous_failure(exception)
                raise

    def stop(self) -> None:
        with self._lock:
            self._ensure_mutable()
            has_running_node = False
            for node in self._nodes:
                cached_state = self._state(node)
                if cached_state is _NodeState.DECOMMISSIONED:
                    continue
                if self._probe_node_running(node):
                    self._set_state(node, _NodeState.RUNNING)
                    has_running_node = True
                else:
                    self._set_state(node, _NodeState.STOPPED)
                    has_running_node = (
                        has_running_node or cached_state is _NodeState.RUNNING
                    )
            if not has_running_node:
                return
            try:
                self._provisioner.stop(self)
                for node in self._nodes:
                    if self._state(node) is not _NodeState.DECOMMISSIONED:
                        self._set_state(node, _NodeState.STOPPED)
            except BaseException as exception:
                self._record_ambiguous_failure(exception)
                raise

    def start_node(self, requested: TestClusterNode) -> None:
        with self._lock:
            self._ensure_mutable()
            node = self._get_node(requested)
            if self._state(node) is _NodeState.DECOMMISSIONED:
                raise RuntimeError(f"Cannot start decommissioned node {node.name}")
            if self._probe_node_running(node):
                self._provisioner.wait_for_node_ready(self, node)
                self._set_state(node, _NodeState.RUNNING)
                return
            self._set_state(node, _NodeState.STOPPED)
            try:
                self._provisioner.start_node(self, node)
                self._set_state(node, _NodeState.RUNNING)
            except BaseException as exception:
                self._record_ambiguous_failure(exception)
                raise

    def stop_node(self, requested: TestClusterNode) -> None:
        with self._lock:
            self._ensure_mutable()
            node = self._get_node(requested)
            state = self._state(node)
            if state is _NodeState.DECOMMISSIONED:
                raise RuntimeError(f"Cannot stop decommissioned node {node.name}")
            if not self._probe_node_running(node) and state is _NodeState.STOPPED:
                self._set_state(node, _NodeState.STOPPED)
                return
            self._set_state(node, _NodeState.RUNNING)
            try:
                self._provisioner.stop_node(self, node)
                self._set_state(node, _NodeState.STOPPED)
            except BaseException as exception:
                self._record_ambiguous_failure(exception)
                raise

    def add_node(self, datacenter: str, rack: str) -> TestClusterNode:
        with self._lock:
            self._ensure_mutable()
            self._ensure_running_seed()
            try:
                node = self._provisioner.add_node(self, datacenter, rack)
                self._nodes.append(node)
                self._set_state(node, _NodeState.RUNNING)
                add_no_proxy_hosts((node.address,))
                return node
            except BaseException as exception:
                if getattr(exception, "recovery_required", False):
                    self._recovery_required = True
                    self._dirty = True
                if getattr(exception, "cluster_state_ambiguous", False):
                    self._dirty = True
                failed_node: object = getattr(exception, "node", None)
                if getattr(exception, "node_remains_provisioned", False) and isinstance(
                    failed_node, TestClusterNode
                ):
                    self._nodes.append(failed_node)
                    self._set_state(failed_node, _NodeState.STOPPED)
                    self._dirty = True
                if not hasattr(exception, "cluster_state_ambiguous"):
                    self._record_ambiguous_failure(exception)
                raise

    def add_node_from_pool(
        self, pool: ClusterPool, datacenter: str, rack: str
    ) -> TestClusterNode:
        with self._lock:
            self._ensure_mutable()
            pool.reserve_additional_private_node(self)
            return self.add_node(datacenter, rack)

    def remove_node(self, requested: TestClusterNode) -> None:
        with self._lock:
            self._ensure_mutable()
            node = self._get_node(requested)
            if (
                self._state(node) is not _NodeState.DECOMMISSIONED
                and self._active_node_count() == 1
            ):
                raise RuntimeError("Cannot remove final node from cluster")
            try:
                if self._state(node) is _NodeState.STOPPED:
                    self._provisioner.start_node(self, node)
                    self._set_state(node, _NodeState.RUNNING)
                if self._state(node) is not _NodeState.DECOMMISSIONED:
                    self._provisioner.decommission_node(self, node)
                    self._set_state(node, _NodeState.DECOMMISSIONED)
                self._provisioner.delete_node_state(self, node)
                self._nodes.remove(node)
                self._node_states.pop(id(node), None)
            except BaseException as exception:
                self._record_ambiguous_failure(exception)
                raise

    def remove_node_from_pool(
        self, pool: ClusterPool, requested: TestClusterNode
    ) -> None:
        self.remove_node(requested)
        pool.release_additional_private_node(self)

    def mark_dirty(self) -> None:
        with self._lock:
            self._dirty = True

    def mark_recovery_required(self) -> None:
        with self._lock:
            self._dirty = True
            self._recovery_required = True

    def remove_physical(self) -> None:
        """Remove whole cluster; sole operation allowed after ambiguous mutation."""
        with self._lock:
            if self._lifecycle_state is _LifecycleState.CLOSED:
                return
            if self._recovery_required:
                raise RuntimeError(
                    f"Cluster {self._instance_id!r} must be recovered by next test process"
                )
            if self._lifecycle_state is _LifecycleState.REMOVING:
                raise RuntimeError("Cluster removal is already in progress")
            self._lifecycle_state = _LifecycleState.REMOVING
            try:
                self._provisioner.remove(self)
                self._lifecycle_state = _LifecycleState.CLOSED
            except BaseException as exception:
                self._lifecycle_state = _LifecycleState.REMOVAL_FAILED
                self._record_ambiguous_failure(exception)
                raise

    def _record_ambiguous_failure(self, failure: BaseException) -> None:
        self._dirty = True
        self._recovery_required = self._recovery_required or bool(
            getattr(failure, "recovery_required", False)
        )

    def _probe_node_running(self, node: TestClusterNode) -> bool:
        try:
            return self._provisioner.is_node_running(self, node)
        except BaseException as exception:
            self._record_ambiguous_failure(exception)
            raise

    def _get_node(self, requested: TestClusterNode) -> TestClusterNode:
        for node in self._nodes:
            if node is requested:
                return node
        raise ValueError(f"Node is not part of cluster: {requested.name}")

    def _state(self, node: TestClusterNode) -> _NodeState:
        return self._node_states[id(node)]

    def _set_state(self, node: TestClusterNode, state: _NodeState) -> None:
        self._node_states[id(node)] = state

    def _active_node_count(self) -> int:
        return sum(
            state is not _NodeState.DECOMMISSIONED
            for state in self._node_states.values()
        )

    def _ensure_running_seed(self) -> None:
        for node in self._nodes:
            if self._state(node) is _NodeState.DECOMMISSIONED:
                continue
            if self._probe_node_running(node):
                self._set_state(node, _NodeState.RUNNING)
                return
            self._set_state(node, _NodeState.STOPPED)
        raise RuntimeError("Cannot add a node without an existing running seed node")

    def _ensure_mutable(self) -> None:
        if self._lifecycle_state is not _LifecycleState.OPEN:
            raise RuntimeError(
                f"Cluster {self._instance_id!r} is closing or already removed"
            )
        if self._dirty:
            raise RuntimeError(
                f"Cluster {self._instance_id!r} has ambiguous state and must be removed"
            )


class ReadOnlyTestCluster:
    """Live view deliberately exposing no lifecycle or topology controls."""

    def __init__(self, cluster: PhysicalTestCluster) -> None:
        self._cluster = cluster

    @property
    def instance_id(self) -> str:
        return self._cluster.instance_id

    @property
    def spec(self) -> ClusterSpec:
        return self._cluster.spec

    @property
    def nodes(self) -> tuple[TestClusterNode, ...]:
        return self._cluster.nodes

    def connection(self, transport: AlternatorTransport) -> AlternatorConnection:
        return self._cluster.connection(transport)

    def client_config(self, transport: AlternatorTransport) -> Config:
        return self._cluster.client_config(transport)

    def create_client(
        self,
        transport: AlternatorTransport,
        **boto_kwargs: Any,  # noqa: ANN401 -- boto kwargs are dynamic
    ) -> DynamoDBClient:
        return self._cluster.create_client(transport, **boto_kwargs)


class TestResourceScope:
    """Per-lease namespace preventing tests sharing cluster from sharing tables."""

    __test__ = False

    def __init__(
        self,
        cluster: PhysicalTestCluster,
        run_id: str,
        lease_id: int,
        cleanup_timeout: float = _DEFAULT_RESOURCE_CLEANUP_TIMEOUT,
    ) -> None:
        if cleanup_timeout <= 0:
            raise ValueError("cleanup_timeout must be positive")
        self._cluster = cluster
        self._cleanup_timeout = cleanup_timeout
        lease_component = f"_{lease_id}_"
        maximum_run_id_length = (
            MAXIMUM_TABLE_NAME_LENGTH
            - _UNIQUE_SUFFIX_LENGTH
            - len("python_it_")
            - len(lease_component)
        )
        self._prefix = (
            "python_it_"
            + self._truncate(self._sanitize(run_id), maximum_run_id_length)
            + lease_component
        )

    @property
    def prefix(self) -> str:
        return self._prefix

    def new_table_name(self, hint: str) -> str:
        suffix = f"_{uuid.uuid4().hex}"
        maximum_hint_length = (
            MAXIMUM_TABLE_NAME_LENGTH - len(self._prefix) - len(suffix)
        )
        return (
            self._prefix
            + self._truncate(self._sanitize(hint), maximum_hint_length)
            + suffix
        )

    def cleanup(self) -> None:
        transport = (
            AlternatorTransport.HTTP
            if AlternatorTransport.HTTP in self._cluster.spec.transports
            else AlternatorTransport.HTTPS
        )
        connection = self._cluster.connection(transport)
        config = connection.client_config()
        api_timeout = min(self._cleanup_timeout, _MAXIMUM_API_CALL_TIMEOUT)
        config = Config(
            **{
                **config.__dict__,
                "timeouts": TimeoutConfig(
                    discovery_seconds=api_timeout,
                    connect_seconds=api_timeout,
                    read_seconds=api_timeout,
                ),
                "retries": RetryConfig(max_attempts=1),
            }
        )
        client = connection.create_client(
            config=config,
        )
        try:
            self.cleanup_tables(client, self._prefix, self._cleanup_timeout)
        finally:
            close_client(client)

    @staticmethod
    def cleanup_tables(client: DynamoDBClient, prefix: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        owned_tables: list[str] = []
        start_name: str | None = None
        while True:
            TestResourceScope._check_deadline(deadline)
            if start_name:
                response = client.list_tables(ExclusiveStartTableName=start_name)
            else:
                response = client.list_tables()
            TestResourceScope._check_deadline(deadline)
            owned_tables.extend(
                name
                for name in response.get("TableNames", [])
                if name.startswith(prefix)
            )
            start_name = response.get("LastEvaluatedTableName")
            if not start_name:
                break

        for table_name in owned_tables:
            TestResourceScope._check_deadline(deadline)
            try:
                client.delete_table(TableName=table_name)
            except client.exceptions.ResourceNotFoundException:
                continue
            while True:
                TestResourceScope._check_deadline(deadline)
                try:
                    client.describe_table(TableName=table_name)
                except client.exceptions.ResourceNotFoundException:
                    break
                time.sleep(min(_POLL_INTERVAL, max(0.0, deadline - time.monotonic())))

    @staticmethod
    def _check_deadline(deadline: float) -> None:
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "Timed out cleaning DynamoDB tables for CCM cluster lease"
            )

    @staticmethod
    def _sanitize(value: str) -> str:
        return "".join(
            character if TestResourceScope._is_allowed(character) else "_"
            for character in value.lower()
        )

    @staticmethod
    def _is_allowed(character: str) -> bool:
        return "a" <= character <= "z" or "0" <= character <= "9" or character in "_-."

    @staticmethod
    def _truncate(value: str, maximum_length: int) -> str:
        return value if len(value) <= maximum_length else value[:maximum_length]


class PrivateClusterControl:
    """Destructive controls available only through private lease."""

    def __init__(self, pool: ClusterPool, cluster: PhysicalTestCluster) -> None:
        self._pool = pool
        self._cluster = cluster

    def start(self) -> None:
        self._cluster.start()

    def stop(self) -> None:
        self._cluster.stop()

    def start_node(self, node: TestClusterNode) -> None:
        self._cluster.start_node(node)

    def stop_node(self, node: TestClusterNode) -> None:
        self._cluster.stop_node(node)

    def add_node(self, datacenter: str, rack: str) -> TestClusterNode:
        return self._cluster.add_node_from_pool(self._pool, datacenter, rack)

    def remove_node(self, node: TestClusterNode) -> None:
        self._cluster.remove_node_from_pool(self._pool, node)


class ReusableClusterLease:
    """Shareable cluster lease with independent resource namespace."""

    def __init__(
        self,
        pool: ClusterPool,
        token: object,
        cluster: PhysicalTestCluster,
        resources: TestResourceScope,
    ) -> None:
        self._pool = pool
        self._token = token
        self._cluster = ReadOnlyTestCluster(cluster)
        self._resources = resources
        self._closed = False
        self._lock = threading.Lock()

    @property
    def cluster(self) -> TestClusterInfo:
        return self._cluster

    @property
    def resources(self) -> TestResourceScope:
        return self._resources

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._pool.release_reusable(self._token, self._resources)
            self._closed = True

    def __enter__(self) -> ReusableClusterLease:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


class PrivateClusterLease:
    """Exclusive cluster lease exposing lifecycle and topology mutation."""

    def __init__(
        self,
        pool: ClusterPool,
        cluster: PhysicalTestCluster,
        resources: TestResourceScope,
    ) -> None:
        self._pool = pool
        self._cluster = cluster
        self._view = ReadOnlyTestCluster(cluster)
        self._control = PrivateClusterControl(pool, cluster)
        self._resources = resources
        self._closed = False
        self._lock = threading.Lock()

    @property
    def cluster(self) -> TestClusterInfo:
        return self._view

    @property
    def control(self) -> PrivateClusterControl:
        return self._control

    @property
    def resources(self) -> TestResourceScope:
        return self._resources

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._pool.release_private(self._cluster)
            self._closed = True

    def __enter__(self) -> PrivateClusterLease:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
