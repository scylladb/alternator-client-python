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

"""Tests for immutable CCM cluster specifications."""

from collections.abc import Callable
from dataclasses import FrozenInstanceError
from typing import Any, cast

import pytest

from tests.testinfra.cluster_spec import (
    AlternatorTransport,
    AuthenticationMode,
    AuthorizationMode,
    ClusterSecuritySpec,
    ClusterSpec,
    ClusterSpecs,
    ClusterTopology,
    DatacenterSpec,
    NodeResources,
    RackSpec,
    parse_yaml_value,
)


def test_defaults_match_java_ccm_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default spec provisions three HTTP/HTTPS nodes with fixed resources."""
    monkeypatch.delenv("SCYLLA_VERSION", raising=False)

    spec = ClusterSpecs.default_spec()

    assert spec.scylla_version == "release:2025.2.5"
    assert spec.topology == ClusterTopology.single_datacenter(3)
    assert spec.topology.node_count == 3
    assert spec.transports == frozenset(AlternatorTransport)
    assert spec.security == ClusterSecuritySpec.DISABLED
    assert spec.resources == NodeResources.DEFAULT == NodeResources(2, 1024)
    assert spec.scylla_yaml_overrides == {}
    assert ClusterSpec.MAXIMUM_NODE_COUNT == 9


def test_default_factory_honors_scylla_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """Environment package selector replaces only default version."""
    monkeypatch.setenv("SCYLLA_VERSION", "release:2099.1.2")

    spec = ClusterSpecs.default_spec()

    assert spec.scylla_version == "release:2099.1.2"
    assert spec.topology == ClusterTopology.single_datacenter(3)


def test_spec_and_nested_values_are_deeply_immutable() -> None:
    """Frozen values snapshot mutable constructor inputs."""
    racks = [RackSpec(2)]
    datacenters = [DatacenterSpec(cast(Any, racks))]
    transports = {AlternatorTransport.HTTP}
    overrides = {"custom_option": "true"}

    spec = ClusterSpec(
        topology=ClusterTopology(cast(Any, datacenters)),
        transports=cast(Any, transports),
        scylla_yaml_overrides=overrides,
    )
    racks.append(RackSpec(1))
    datacenters.append(DatacenterSpec.create(1))
    transports.add(AlternatorTransport.HTTPS)
    overrides["other_option"] = "false"

    assert spec.topology == ClusterTopology.single_datacenter(2)
    assert spec.transports == frozenset({AlternatorTransport.HTTP})
    assert spec.scylla_yaml_overrides == {"custom_option": "true"}
    with pytest.raises(FrozenInstanceError):
        spec.scylla_version = "release:changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        spec.scylla_yaml_overrides["other_option"] = "false"  # type: ignore[index]


@pytest.mark.parametrize(
    ("authentication", "authorization", "enforced"),
    [
        (authentication, authorization, enforced)
        for authentication in AuthenticationMode
        for authorization in AuthorizationMode
        for enforced in (False, True)
    ],
)
def test_security_accepts_exact_supported_combinations(
    authentication: AuthenticationMode,
    authorization: AuthorizationMode,
    enforced: bool,
) -> None:
    """Security validation matches Java contract for every enum combination."""
    expected_valid = (
        authentication is not AuthenticationMode.ALLOW_ALL
        or authorization is AuthorizationMode.ALLOW_ALL
    ) and (
        not enforced
        or (
            authentication is AuthenticationMode.PASSWORD
            and authorization is AuthorizationMode.CASSANDRA
        )
    )

    if expected_valid:
        security = ClusterSecuritySpec(authentication, authorization, enforced)
        assert security.authentication is authentication
        assert security.authorization is authorization
        assert security.enforce_alternator_authorization is enforced
    else:
        with pytest.raises(ValueError):
            ClusterSecuritySpec(authentication, authorization, enforced)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: RackSpec(0),
        lambda: DatacenterSpec(cast(Any, ())),
        lambda: ClusterTopology(cast(Any, ())),
        lambda: NodeResources(0, 1024),
        lambda: NodeResources(2, -1),
        lambda: ClusterSpec(topology=ClusterTopology.single_datacenter(10)),
        lambda: ClusterSpec(transports=frozenset()),
        lambda: ClusterSpec(scylla_version="\u2003"),
    ],
)
def test_invalid_cluster_shapes_fail_before_provisioning(
    factory: Callable[[], object],
) -> None:
    """Empty, oversized, or non-positive typed settings fail eagerly."""
    with pytest.raises(ValueError):
        factory()


def test_topology_supports_datacenters_and_racks_but_rejects_integer_overflow() -> None:
    """Topology counts every rack and retains Java integer-overflow guard."""
    topology = ClusterTopology(
        (
            DatacenterSpec.create(2, 1),
            DatacenterSpec.create(3, 2),
        )
    )
    assert topology.node_count == 8

    with pytest.raises(ValueError, match="2147483647"):
        ClusterTopology(
            (
                DatacenterSpec((RackSpec(2_147_483_647),)),
                DatacenterSpec.create(1),
            )
        )


def test_with_methods_return_validated_copies_without_changing_source() -> None:
    """Every typed fluent method preserves original frozen specification."""
    original = ClusterSpec()
    security = ClusterSecuritySpec(
        AuthenticationMode.TRANSITIONAL,
        AuthorizationMode.TRANSITIONAL,
        False,
    )

    changed = (
        original.with_scylla_version("release:2026.1")
        .with_topology(ClusterTopology.single_datacenter(1, 2))
        .with_transports(AlternatorTransport.HTTPS)
        .with_security(security)
        .with_resources(NodeResources(1, 512))
        .with_yaml_override("custom_option", "[one, two]")
    )

    assert original == ClusterSpec()
    assert changed.scylla_version == "release:2026.1"
    assert changed.topology == ClusterTopology.single_datacenter(1, 2)
    assert changed.transports == frozenset({AlternatorTransport.HTTPS})
    assert changed.security == security
    assert changed.resources == NodeResources(1, 512)
    assert changed.scylla_yaml_overrides == {"custom_option": "[one, two]"}
    with pytest.raises(ValueError, match="At least one"):
        original.with_transports()


def test_yaml_keys_are_trimmed_canonicalized_sorted_and_immutable() -> None:
    """Canonical map ignores insertion order and replaces equivalent keys."""
    padded = (
        ClusterSpec()
        .with_yaml_override("\u2003\u00a0custom_option\u3000", "first")
        .with_yaml_override("second_option", "2")
        .with_yaml_override("custom_option", "second")
    )
    reordered = ClusterSpec(
        scylla_yaml_overrides={
            "second_option": "2",
            "custom_option": "second",
        }
    )

    assert list(padded.scylla_yaml_overrides) == ["custom_option", "second_option"]
    assert padded.scylla_yaml_overrides == {
        "custom_option": "second",
        "second_option": "2",
    }
    assert padded.reuse_key == reordered.reuse_key
    assert hash(padded) == hash(reordered)


@pytest.mark.parametrize(
    "overrides",
    [
        {"custom_option": "true", " custom_option ": "false"},
        {" custom_option ": "false", "custom_option": "true"},
    ],
)
def test_yaml_keys_cannot_collide_after_canonicalization(
    overrides: dict[str, str],
) -> None:
    """Constructor maps cannot use insertion order to resolve canonical keys."""
    with pytest.raises(ValueError, match="after canonicalization"):
        ClusterSpec(scylla_yaml_overrides=overrides)


@pytest.mark.parametrize(
    ("alias", "canonical"),
    [
        ("cql_port", "native_transport_port"),
        ("datadir", "data_file_directories"),
    ],
)
def test_yaml_aliases_cannot_bypass_typed_roots(alias: str, canonical: str) -> None:
    """CCM aliases resolve before typed-key reservation checks."""
    with pytest.raises(ValueError, match=canonical):
        ClusterSpec().with_yaml_override(alias, "false")


@pytest.mark.parametrize(
    "key",
    [
        "alternator_enforce_authorization ",
        "\u00a0alternator_encryption_options.enabled\u2003",
        "api_address",
        "api_port",
        "auto_bootstrap",
        "blocked_reactor_notify_ms",
        "initial_token",
        "kernel_page_cache",
        "native_transport_port",
        "data_file_directories",
        "commitlog_directory",
        "developer_mode",
        "endpoint_snitch",
        "maintenance_socket",
        "max_networking_io_control_blocks",
        "overprovisioned",
        "prometheus_port",
        "replace_address_first_boot",
        "server_encryption_options",
        "smp",
        "memory",
        "storage_port",
        "unsafe_bypass_fsync",
    ],
)
def test_typed_yaml_roots_cannot_be_overridden(key: str) -> None:
    """Free-form YAML cannot replace behavior owned by typed provisioning."""
    with pytest.raises(ValueError, match="owned by a typed cluster option"):
        ClusterSpec().with_yaml_override(key, "false")


@pytest.mark.parametrize(
    "key",
    [
        cast(Any, None),
        "",
        " ",
        "\u200b",
        ".option",
        "option.",
        "parent..child",
        "parent.child.grandchild",
        "option:value",
        "option\ninjected",
        "\toption",
        "option-name",
        "option name",
        "na\u00efve",
        "1option",
    ],
)
def test_unsafe_or_unsupported_yaml_keys_are_rejected(key: str) -> None:
    """YAML keys allow only one or two ASCII identifier segments."""
    with pytest.raises((TypeError, ValueError)):
        ClusterSpec().with_yaml_override(key, "true")


def test_safe_top_level_and_nested_yaml_keys_are_accepted() -> None:
    """Supported ASCII keys retain YAML text for later CCM translation."""
    spec = (
        ClusterSpec()
        .with_yaml_override("_custom2", "true")
        .with_yaml_override("custom_group.child_2", "42")
    )

    assert spec.scylla_yaml_overrides == {
        "_custom2": "true",
        "custom_group.child_2": "42",
    }


def test_nested_override_requires_mapping_root() -> None:
    """Dotted child cannot be applied below scalar root override."""
    with pytest.raises(ValueError, match="must be a mapping"):
        (
            ClusterSpec()
            .with_yaml_override("custom_group", "5")
            .with_yaml_override("custom_group.child", "42")
        )

    spec = (
        ClusterSpec()
        .with_yaml_override("custom_group", "{existing: true}")
        .with_yaml_override("custom_group.child", "42")
    )
    assert len(spec.scylla_yaml_overrides) == 2


@pytest.mark.parametrize(
    "value",
    [
        "[",
        "{key: value",
        "'unterminated",
        "a: 1\na: 2",
        "--- true\n--- false",
        "!!python/object:builtins.object {}",
        "\u2003\u00a0",
    ],
)
def test_invalid_or_unsafe_yaml_values_are_rejected(value: str) -> None:
    """Overrides require one safe, non-empty, duplicate-free YAML value."""
    with pytest.raises(ValueError, match="empty|invalid YAML"):
        ClusterSpec().with_yaml_override("custom_option", value)


def test_yaml_values_use_yaml_12_core_scalar_semantics() -> None:
    """CCM values avoid PyYAML's legacy YAML 1.1 bool and octal rules."""
    assert parse_yaml_value("0123") == 123
    assert parse_yaml_value("0o123") == 83
    assert parse_yaml_value("yes") == "yes"
    assert parse_yaml_value("on") == "on"
    assert parse_yaml_value("1:20") == "1:20"
    assert parse_yaml_value("2026-09-12") == "2026-09-12"
    assert parse_yaml_value("true") is True
    assert parse_yaml_value("1e3") == 1000.0
    assert parse_yaml_value("null") is None
    assert parse_yaml_value("[one, two]") == ["one", "two"]
    assert parse_yaml_value("{one: 1, two: false}") == {
        "one": 1,
        "two": False,
    }


def test_reuse_key_contains_every_behavior_affecting_setting() -> None:
    """Changing any typed or free-form option changes physical reuse identity."""
    base = ClusterSpec()
    variants = [
        base.with_scylla_version("release:2025.2.6"),
        base.with_topology(ClusterTopology.single_datacenter(2, 1)),
        base.with_transports(AlternatorTransport.HTTP),
        base.with_security(
            ClusterSecuritySpec(
                AuthenticationMode.PASSWORD,
                AuthorizationMode.ALLOW_ALL,
                False,
            )
        ),
        base.with_resources(NodeResources(1, 1024)),
        base.with_yaml_override("custom_option", "true"),
    ]

    assert all(variant.reuse_key != base.reuse_key for variant in variants)
    assert len({variant.reuse_key for variant in variants}) == len(variants)


def test_reuse_key_length_frames_delimiter_containing_values() -> None:
    """Version and YAML length framing prevents delimiter confusion."""
    first = ClusterSpec().with_scylla_version(
        "X|[HTTP, HTTPS]|ALLOW_ALL|ALLOW_ALL|false|2|1024|dc:3|"
        "yaml:16:logger_log_level=59:info # "
    )
    second = (
        ClusterSpec()
        .with_scylla_version("X")
        .with_yaml_override(
            "logger_log_level",
            "info # |[HTTP, HTTPS]|ALLOW_ALL|ALLOW_ALL|false|2|1024|dc:3",
        )
    )

    assert first.reuse_key != second.reuse_key


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"topology": cast(Any, "dc1")}, "topology"),
        ({"transports": cast(Any, {"http"})}, "transports"),
        ({"security": cast(Any, "disabled")}, "security"),
        ({"resources": cast(Any, (2, 1024))}, "resources"),
        ({"scylla_yaml_overrides": cast(Any, [])}, "mapping"),
    ],
)
def test_wrong_runtime_types_fail_clearly(
    kwargs: dict[str, object], message: str
) -> None:
    """Python callers get eager failures instead of late provisioner crashes."""
    with pytest.raises(TypeError, match=message):
        ClusterSpec(**cast(Any, kwargs))
