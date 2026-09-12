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

"""Immutable cluster specifications for CCM-backed integration tests."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import Enum
from types import MappingProxyType
from typing import ClassVar, Protocol, cast

import yaml

__all__ = [
    "AlternatorTransport",
    "AuthenticationMode",
    "AuthorizationMode",
    "ClusterSecuritySpec",
    "ClusterSpec",
    "ClusterSpecs",
    "ClusterTopology",
    "DatacenterSpec",
    "NodeResources",
    "RackSpec",
    "parse_yaml_value",
]

_MAXIMUM_SIGNED_INT = 2_147_483_647
_YAML_BOOLEAN_TAG = "tag:yaml.org,2002:bool"
_YAML_FLOAT_TAG = "tag:yaml.org,2002:float"
_YAML_INTEGER_TAG = "tag:yaml.org,2002:int"
_YAML_MAPPING_TAG = "tag:yaml.org,2002:map"
_YAML_TIMESTAMP_TAG = "tag:yaml.org,2002:timestamp"
_YAML_11_ONLY_OR_REPLACED_TAGS = frozenset(
    {
        _YAML_BOOLEAN_TAG,
        _YAML_FLOAT_TAG,
        _YAML_INTEGER_TAG,
        _YAML_TIMESTAMP_TAG,
    }
)

_YAML_BOOLEAN_PATTERN = re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$")
_YAML_INTEGER_PATTERN = re.compile(
    r"^(?:[-+]?0b[0-1_]+|[-+]?0o[0-7_]+|[-+]?[0-9][0-9_]*|"
    r"[-+]?0x[0-9a-fA-F_]+)$"
)
_YAML_FLOAT_PATTERN = re.compile(
    r"^(?:"
    r"[-+]?(?:(?:[0-9][0-9_]*)?\.[0-9_]+|[0-9][0-9_]*\.)"
    r"(?:[eE][-+]?[0-9]+)?|"
    r"[-+]?[0-9][0-9_]*[eE][-+]?[0-9]+|"
    r"[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN)"
    r")$"
)

_YAML_KEY_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?\Z")
_SCYLLA_YAML_KEY_ALIASES = {
    "cql_port": "native_transport_port",
    "datadir": "data_file_directories",
}
_RESERVED_YAML_KEYS = frozenset(
    {
        "alternator_address",
        "alternator_encryption_options",
        "alternator_enforce_authorization",
        "alternator_https_port",
        "alternator_port",
        "alternator_write_isolation",
        "api_address",
        "api_port",
        "auth_superuser_name",
        "auth_superuser_salted_password",
        "authenticator",
        "auto_bootstrap",
        "authorizer",
        "blocked_reactor_notify_ms",
        "broadcast_address",
        "broadcast_rpc_address",
        "cluster_name",
        "commitlog_directory",
        "commitlog_use_o_dsync",
        "data_file_directories",
        "default_log_level",
        "developer_mode",
        "endpoint_snitch",
        "hints_directory",
        "ignore_dead_nodes_for_replace",
        "initial_token",
        "join_ring",
        "kernel_page_cache",
        "listen_address",
        "listen_interface",
        "listen_interface_prefer_ipv6",
        "listen_on_broadcast_address",
        "load_ring_state",
        "log_to_stdout",
        "maintenance_mode",
        "maintenance_socket",
        "maintenance_socket_group",
        "max_networking_io_control_blocks",
        "memory",
        "native_shard_aware_transport_port",
        "native_shard_aware_transport_port_proxy_protocol",
        "native_shard_aware_transport_port_ssl",
        "native_shard_aware_transport_port_ssl_proxy_protocol",
        "native_transport_port",
        "native_transport_port_ssl",
        "num_tokens",
        "overprovisioned",
        "partitioner",
        "prometheus_address",
        "prometheus_port",
        "redis_port",
        "redis_ssl_port",
        "replace_address",
        "replace_address_first_boot",
        "replace_node_first_boot",
        "role_manager",
        "rpc_address",
        "rpc_interface",
        "rpc_interface_prefer_ipv6",
        "rpc_port",
        "saved_caches_directory",
        "schema_commitlog_directory",
        "seeds",
        "seed_provider",
        "server_encryption_options",
        "smp",
        "ssl_storage_port",
        "start_native_transport",
        "storage_port",
        "unsafe_bypass_fsync",
        "view_hints_directory",
        "workdir",
    }
)


class _YamlLoader(Protocol):
    """Operations used by custom PyYAML constructors."""

    def construct_scalar(self, node: object) -> str:
        """Construct one scalar value."""

    def construct_object(self, node: object, deep: bool = False) -> object:
        """Construct one arbitrary YAML value."""

    def flatten_mapping(self, node: object) -> None:
        """Expand YAML mapping merges before construction."""


class _MappingNode(Protocol):
    """Subset of a PyYAML mapping node used by duplicate-key validation."""

    value: list[tuple[object, object]]


class _Yaml12SafeLoader(yaml.SafeLoader):
    """Safe PyYAML loader whose scalar resolution follows YAML 1.2 Core."""

    yaml_implicit_resolvers = {
        character: [
            resolver
            for resolver in resolvers
            if resolver[0] not in _YAML_11_ONLY_OR_REPLACED_TAGS
        ]
        for character, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }


def _construct_yaml12_integer(loader: _YamlLoader, node: object) -> int:
    """Construct YAML 1.2 binary, octal, decimal, or hexadecimal integer."""
    value = loader.construct_scalar(node).replace("_", "")
    sign = 1
    if value.startswith("-"):
        sign = -1
        value = value[1:]
    elif value.startswith("+"):
        value = value[1:]

    if value.startswith("0b"):
        return sign * int(value[2:], 2)
    if value.startswith("0o"):
        return sign * int(value[2:], 8)
    if value.startswith("0x"):
        return sign * int(value[2:], 16)
    return sign * int(value, 10)


def _construct_unique_mapping(
    loader: _YamlLoader, node: object, deep: bool = False
) -> dict[object, object]:
    """Construct mapping while rejecting duplicate or unhashable keys."""
    loader.flatten_mapping(node)
    mapping_node = cast(_MappingNode, node)
    result: dict[object, object] = {}
    for key_node, value_node in mapping_node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as exception:
            raise ValueError("YAML mapping contains an unhashable key") from exception
        if duplicate:
            raise ValueError(f"YAML mapping contains duplicate key {key!r}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_Yaml12SafeLoader.add_implicit_resolver(
    _YAML_BOOLEAN_TAG, _YAML_BOOLEAN_PATTERN, list("tTfF")
)
_Yaml12SafeLoader.add_implicit_resolver(
    _YAML_INTEGER_TAG, _YAML_INTEGER_PATTERN, list("-+0123456789")
)
_Yaml12SafeLoader.add_implicit_resolver(
    _YAML_FLOAT_TAG, _YAML_FLOAT_PATTERN, list("-+0123456789.")
)
_Yaml12SafeLoader.add_constructor(  # type: ignore[type-var]  # PyYAML stubs reject valid custom loader protocol
    _YAML_INTEGER_TAG,
    _construct_yaml12_integer,
)
_Yaml12SafeLoader.add_constructor(  # type: ignore[type-var]  # PyYAML stubs reject valid custom loader protocol
    _YAML_MAPPING_TAG,
    _construct_unique_mapping,
)


def parse_yaml_value(value: str) -> object:
    """Parse one YAML value using safe YAML 1.2 Core scalar semantics."""
    if not isinstance(value, str):
        raise TypeError("A Scylla YAML override value must be a string")
    return cast(object, yaml.load(value, Loader=_Yaml12SafeLoader))


class AlternatorTransport(Enum):
    """Alternator transports exposed by a test cluster."""

    HTTP = "http"
    HTTPS = "https"


class AuthenticationMode(Enum):
    """Authentication backends supported by the CCM harness."""

    ALLOW_ALL = "allow_all"
    PASSWORD = "password"
    TRANSITIONAL = "transitional"


class AuthorizationMode(Enum):
    """Authorization backends supported by the CCM harness."""

    ALLOW_ALL = "allow_all"
    CASSANDRA = "cassandra"
    TRANSITIONAL = "transitional"


@dataclass(frozen=True, slots=True)
class ClusterSecuritySpec:
    """Typed authentication and authorization settings for a test cluster."""

    authentication: AuthenticationMode
    authorization: AuthorizationMode
    enforce_alternator_authorization: bool

    DISABLED: ClassVar[ClusterSecuritySpec]
    ENFORCED: ClassVar[ClusterSecuritySpec]

    def __post_init__(self) -> None:
        """Validate backend types and supported security combinations."""
        if not isinstance(self.authentication, AuthenticationMode):
            raise TypeError("authentication must be an AuthenticationMode")
        if not isinstance(self.authorization, AuthorizationMode):
            raise TypeError("authorization must be an AuthorizationMode")
        if not isinstance(self.enforce_alternator_authorization, bool):
            raise TypeError("enforce_alternator_authorization must be a bool")
        if (
            self.authentication is AuthenticationMode.ALLOW_ALL
            and self.authorization is not AuthorizationMode.ALLOW_ALL
        ):
            raise ValueError(
                "Allow-all authentication can only be used with allow-all authorization"
            )
        if self.enforce_alternator_authorization and (
            self.authentication is not AuthenticationMode.PASSWORD
            or self.authorization is not AuthorizationMode.CASSANDRA
        ):
            raise ValueError(
                "Alternator authorization enforcement requires password "
                "authentication and Cassandra authorization"
            )


ClusterSecuritySpec.DISABLED = ClusterSecuritySpec(
    AuthenticationMode.ALLOW_ALL,
    AuthorizationMode.ALLOW_ALL,
    False,
)
ClusterSecuritySpec.ENFORCED = ClusterSecuritySpec(
    AuthenticationMode.PASSWORD,
    AuthorizationMode.CASSANDRA,
    True,
)


@dataclass(frozen=True, slots=True)
class NodeResources:
    """Per-node Scylla CPU and memory configuration."""

    smp: int
    memory_mib: int

    DEFAULT: ClassVar[NodeResources]

    def __post_init__(self) -> None:
        """Require positive integer CPU and memory values."""
        if (
            not isinstance(self.smp, int)
            or isinstance(self.smp, bool)
            or not isinstance(self.memory_mib, int)
            or isinstance(self.memory_mib, bool)
            or self.smp < 1
            or self.memory_mib < 1
        ):
            raise ValueError("Node SMP and memory must be positive integers")


NodeResources.DEFAULT = NodeResources(2, 1024)


@dataclass(frozen=True, slots=True)
class RackSpec:
    """Number of nodes in one rack."""

    node_count: int

    def __post_init__(self) -> None:
        """Require at least one node."""
        if (
            not isinstance(self.node_count, int)
            or isinstance(self.node_count, bool)
            or self.node_count < 1
        ):
            raise ValueError("A rack must contain at least one node")


@dataclass(frozen=True, slots=True)
class DatacenterSpec:
    """Immutable rack layout for one datacenter."""

    racks: tuple[RackSpec, ...]

    def __post_init__(self) -> None:
        """Snapshot and validate rack definitions."""
        try:
            racks = tuple(self.racks)
        except TypeError as exception:
            raise TypeError(
                "racks must be an iterable of RackSpec values"
            ) from exception
        if not racks:
            raise ValueError("A datacenter must contain at least one rack")
        if any(not isinstance(rack, RackSpec) for rack in racks):
            raise TypeError("racks must contain only RackSpec values")
        object.__setattr__(self, "racks", racks)

    @classmethod
    def create(cls, *nodes_per_rack: int) -> DatacenterSpec:
        """Build one datacenter from per-rack node counts."""
        return cls(tuple(RackSpec(count) for count in nodes_per_rack))


@dataclass(frozen=True, slots=True)
class ClusterTopology:
    """Immutable datacenter and rack layout for a cluster."""

    datacenters: tuple[DatacenterSpec, ...]

    def __post_init__(self) -> None:
        """Snapshot and validate datacenter definitions."""
        try:
            datacenters = tuple(self.datacenters)
        except TypeError as exception:
            raise TypeError(
                "datacenters must be an iterable of DatacenterSpec values"
            ) from exception
        if not datacenters:
            raise ValueError("A cluster must contain at least one datacenter")
        if any(
            not isinstance(datacenter, DatacenterSpec) for datacenter in datacenters
        ):
            raise TypeError("datacenters must contain only DatacenterSpec values")
        object.__setattr__(self, "datacenters", datacenters)
        # Force overflow validation during construction, matching Java's eager checks.
        _ = self.node_count

    @classmethod
    def single_datacenter(cls, *nodes_per_rack: int) -> ClusterTopology:
        """Build a one-datacenter topology from per-rack node counts."""
        return cls((DatacenterSpec.create(*nodes_per_rack),))

    @property
    def node_count(self) -> int:
        """Return total nodes, rejecting values beyond Java's integer range."""
        count = 0
        for datacenter in self.datacenters:
            for rack in datacenter.racks:
                count += rack.node_count
                if count > _MAXIMUM_SIGNED_INT:
                    raise ValueError(
                        "A cluster topology cannot contain more than "
                        f"{_MAXIMUM_SIGNED_INT} nodes"
                    )
        return count


def _default_topology() -> ClusterTopology:
    """Create default three-node topology."""
    return ClusterTopology.single_datacenter(3)


def _default_transports() -> frozenset[AlternatorTransport]:
    """Create default HTTP-and-HTTPS transport set."""
    return frozenset(AlternatorTransport)


def _canonicalize_yaml_key(key: str) -> str:
    """Validate, trim, alias, and reserve one Scylla YAML key."""
    if not isinstance(key, str):
        raise TypeError("A Scylla YAML override key must be a string")
    if any(
        ord(character) <= 0x1F or 0x7F <= ord(character) <= 0x9F for character in key
    ):
        raise ValueError("Scylla YAML override keys cannot contain controls")

    canonical = key.strip()
    if _YAML_KEY_PATTERN.fullmatch(canonical) is None:
        raise ValueError(
            "A Scylla YAML override key must contain one or two ASCII "
            "identifier segments"
        )

    root, separator, child = canonical.partition(".")
    root = _SCYLLA_YAML_KEY_ALIASES.get(root, root)
    if root in _RESERVED_YAML_KEYS:
        raise ValueError(f"Scylla YAML key {root!r} is owned by a typed cluster option")
    return root if not separator else f"{root}.{child}"


def _canonicalize_yaml_overrides(
    overrides: Mapping[str, str],
) -> Mapping[str, str]:
    """Return sorted, immutable, validated YAML overrides."""
    if not isinstance(overrides, Mapping):
        raise TypeError("scylla_yaml_overrides must be a mapping")

    canonical: dict[str, str] = {}
    for key, value in overrides.items():
        canonical_key = _canonicalize_yaml_key(key)
        if canonical_key in canonical:
            raise ValueError(
                f"Duplicate Scylla YAML override key {canonical_key!r} "
                "after canonicalization"
            )
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Scylla YAML override keys and values cannot be empty")
        try:
            parse_yaml_value(value)
        except Exception as exception:
            raise ValueError(
                f"Scylla YAML override {canonical_key!r} has an invalid YAML value"
            ) from exception
        canonical[canonical_key] = value

    ordered = dict(sorted(canonical.items()))
    parsed = {key: parse_yaml_value(value) for key, value in ordered.items()}
    for key in parsed:
        root, separator, _child = key.partition(".")
        if separator and root in parsed and not isinstance(parsed[root], Mapping):
            raise ValueError(
                f"Scylla YAML override {root!r} must be a mapping when "
                f"overriding {key!r}"
            )
    return MappingProxyType(ordered)


@dataclass(frozen=True, slots=True)
class ClusterSpec:
    """Complete immutable description of a CCM-provisioned Scylla cluster."""

    MAXIMUM_NODE_COUNT: ClassVar[int] = 9
    DEFAULT_SCYLLA_VERSION: ClassVar[str] = "release:2025.2.5"

    scylla_version: str = DEFAULT_SCYLLA_VERSION
    topology: ClusterTopology = field(default_factory=_default_topology)
    transports: frozenset[AlternatorTransport] = field(
        default_factory=_default_transports
    )
    security: ClusterSecuritySpec = field(
        default_factory=lambda: ClusterSecuritySpec.DISABLED
    )
    resources: NodeResources = field(default_factory=lambda: NodeResources.DEFAULT)
    scylla_yaml_overrides: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Snapshot collections and validate every provisioning option."""
        if not isinstance(self.scylla_version, str) or not self.scylla_version.strip():
            raise ValueError("A Scylla version is required")
        if not isinstance(self.topology, ClusterTopology):
            raise TypeError("topology must be a ClusterTopology")
        try:
            transports = frozenset(self.transports)
        except TypeError as exception:
            raise TypeError(
                "transports must be an iterable of AlternatorTransport values"
            ) from exception
        if not transports:
            raise ValueError("At least one Alternator transport is required")
        if any(
            not isinstance(transport, AlternatorTransport) for transport in transports
        ):
            raise TypeError("transports must contain only AlternatorTransport values")
        if not isinstance(self.security, ClusterSecuritySpec):
            raise TypeError("security must be a ClusterSecuritySpec")
        if not isinstance(self.resources, NodeResources):
            raise TypeError("resources must be a NodeResources")
        if self.topology.node_count > self.MAXIMUM_NODE_COUNT:
            raise ValueError(f"A cluster cannot exceed {self.MAXIMUM_NODE_COUNT} nodes")

        object.__setattr__(self, "transports", transports)
        object.__setattr__(
            self,
            "scylla_yaml_overrides",
            _canonicalize_yaml_overrides(self.scylla_yaml_overrides),
        )

    def _identity(self) -> tuple[object, ...]:
        """Return canonical state shared by equality and hashing."""
        return (
            self.scylla_version,
            self.topology,
            self.transports,
            self.security,
            self.resources,
            tuple(self.scylla_yaml_overrides.items()),
        )

    def __eq__(self, other: object) -> bool:
        """Compare canonical behavior-affecting settings."""
        if not isinstance(other, ClusterSpec):
            return NotImplemented
        return self._identity() == other._identity()

    def __hash__(self) -> int:
        """Hash canonical behavior-affecting settings."""
        return hash(self._identity())

    @property
    def reuse_key(self) -> str:
        """Return deterministic identity containing every provisioning setting."""
        transport_names = ", ".join(
            transport.name
            for transport in AlternatorTransport
            if transport in self.transports
        )
        pieces = [
            f"{len(self.scylla_version)}:{self.scylla_version}",
            f"[{transport_names}]",
            self.security.authentication.name,
            self.security.authorization.name,
            str(self.security.enforce_alternator_authorization).lower(),
            str(self.resources.smp),
            str(self.resources.memory_mib),
        ]
        for datacenter in self.topology.datacenters:
            pieces.append(
                "dc" + "".join(f":{rack.node_count}" for rack in datacenter.racks)
            )
        for key, value in self.scylla_yaml_overrides.items():
            pieces.append(f"yaml:{len(key)}:{key}={len(value)}:{value}")
        return "|".join(pieces)

    def with_scylla_version(self, value: str) -> ClusterSpec:
        """Return copy selecting another Scylla package."""
        return replace(self, scylla_version=value)

    def with_topology(self, value: ClusterTopology) -> ClusterSpec:
        """Return copy selecting another cluster topology."""
        return replace(self, topology=value)

    def with_transports(self, *values: AlternatorTransport) -> ClusterSpec:
        """Return copy exposing one or more Alternator transports."""
        if not values:
            raise ValueError("At least one Alternator transport is required")
        return replace(self, transports=frozenset(values))

    def with_security(self, value: ClusterSecuritySpec) -> ClusterSpec:
        """Return copy selecting another authentication profile."""
        return replace(self, security=value)

    def with_resources(self, value: NodeResources) -> ClusterSpec:
        """Return copy selecting another per-node resource configuration."""
        return replace(self, resources=value)

    def with_yaml_override(self, key: str, yaml_value: str) -> ClusterSpec:
        """Return copy adding or replacing one canonical YAML override."""
        canonical_key = _canonicalize_yaml_key(key)
        overrides = dict(self.scylla_yaml_overrides)
        overrides[canonical_key] = yaml_value
        return replace(self, scylla_yaml_overrides=overrides)


class ClusterSpecs:
    """Factory for common cluster specifications."""

    @staticmethod
    def default_spec() -> ClusterSpec:
        """Return defaults with optional ``SCYLLA_VERSION`` selection."""
        return ClusterSpec(
            scylla_version=os.environ.get(
                "SCYLLA_VERSION", ClusterSpec.DEFAULT_SCYLLA_VERSION
            )
        )
