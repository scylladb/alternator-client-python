# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure Python preflight; unsupported settings cannot start discovery."""

from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

import pytest

from alternator import (
    TLS,
    Auth,
    ClusterScope,
    Config,
    DatacenterScope,
    KeyRouteAffinityConfig,
    KeyRouteAffinityMode,
    RackScope,
    TimeoutConfig,
)
from alternator._experimental.rust._config import prepare_config
from alternator.config import (
    DEFAULT_USER_AGENT,
    NodeListPollingConfig,
    RequestCompressionConfig,
    RetryConfig,
)
from alternator.core.routing_scope import RoutingScope
from alternator.exceptions import ConfigurationError
from tests.contract.config_cases import rejected_native_configs


@pytest.mark.parametrize(
    "case",
    rejected_native_configs(Config(["127.0.0.1"], 8000)),
    ids=lambda case: case.name,
)
def test_rejected_contract_settings_before_construction(case: object) -> None:
    # This layer imports neither the extension nor a Python SDK transport.
    from tests.contract.config_cases import RejectedConfig

    assert isinstance(case, RejectedConfig)
    with pytest.raises(ConfigurationError):
        prepare_config(case.config)


def test_callback_runs_once_and_copies_are_independent() -> None:
    calls: list[str] = []

    def agent(default: str) -> str:
        calls.append(default)
        return "frozen-agent/1"

    seeds = ["127.0.0.1"]
    config = Config(
        seeds,
        8000,
        user_agent=agent,
        routing_scope=RackScope(
            "dc", "rack", fallback=DatacenterScope("other", fallback=ClusterScope())
        ),
        key_affinity=KeyRouteAffinityConfig(
            mode=KeyRouteAffinityMode.RMW, table_pk_attributes={"table": "pk"}
        ),
    )
    auth = Auth.static_credentials("key", "secret", "token")
    prepared = prepare_config(config, auth)
    seeds.append("127.0.0.2")
    assert isinstance(config.key_affinity.table_pk_attributes, dict)
    config.key_affinity.table_pk_attributes["table"] = "changed"
    native = prepared.native_config()
    assert native["seed_hosts"] == ["127.0.0.1"]
    assert native["scopes"] == [
        ("rack", "dc", "rack"),
        ("datacenter", "other", None),
        ("cluster", None, None),
    ]
    assert native["table_pk_attributes"] == {"table": "pk"}
    assert native["user_agent"] == "frozen-agent/1"
    assert native["session_token"] == "token"
    assert native["discovery_seconds"] == 5.0
    assert "max_decompressed_response_bytes" not in native
    native["table_pk_attributes"]["table"] = "mutated"
    native["seed_hosts"].clear()
    delegate = prepared.delegate_config()
    assert isinstance(delegate.key_affinity.table_pk_attributes, dict)
    delegate.key_affinity.table_pk_attributes.clear()
    assert prepared.native_config()["table_pk_attributes"] == {"table": "pk"}
    assert prepared.delegate_config().user_agent == "frozen-agent/1"
    assert prepared.delegate_config().parameter_validation is True
    assert prepared.delegate_config().max_decompressed_response_bytes == 67_108_864
    assert prepared.auth == auth
    assert calls == [DEFAULT_USER_AGENT]


@pytest.mark.parametrize(
    "key", ["pk\nname", "pk\x00name", "pk\x7fname", "pk\x85name", "分区🔑"]
)
def test_attribute_names_preserved_in_frozen_snapshots(key: str) -> None:
    config = Config(
        ["127.0.0.1"],
        8000,
        key_affinity=KeyRouteAffinityConfig(table_pk_attributes={"table": key}),
    )
    prepared = prepare_config(config)
    assert isinstance(config.key_affinity.table_pk_attributes, dict)
    config.key_affinity.table_pk_attributes["table"] = "changed"

    native = prepared.native_config()
    delegate = prepared.delegate_config()
    assert native["table_pk_attributes"] == {"table": key}
    assert delegate.key_affinity.table_pk_attributes == {"table": key}

    native["table_pk_attributes"].clear()
    assert isinstance(delegate.key_affinity.table_pk_attributes, dict)
    delegate.key_affinity.table_pk_attributes.clear()
    assert prepared.native_config()["table_pk_attributes"] == {"table": key}
    assert prepared.delegate_config().key_affinity.table_pk_attributes == {"table": key}


def test_native_numeric_widths_accept_values_above_old_caps() -> None:
    config = Config(
        ["127.0.0.1"],
        8000,
        node_list_polling=NodeListPollingConfig(2**31, 2**64 - 1),
        request_compression=RequestCompressionConfig(
            min_size_bytes=2 * sys.maxsize + 1
        ),
        retries=RetryConfig(max_attempts=2**32 - 1),
        timeouts=TimeoutConfig(read_seconds=2**32),
    )
    mapping = prepare_config(config).native_config()
    assert mapping["active_interval_ms"] == 2**31
    assert mapping["idle_interval_ms"] == 2**64 - 1
    assert mapping["compression_min_bytes"] == 2 * sys.maxsize + 1
    assert mapping["max_attempts"] == 2**32 - 1
    assert mapping["read_seconds"] == 2**32


@pytest.mark.parametrize(
    "config",
    [
        Config(["127.0.0.1"], 8000, node_list_polling=NodeListPollingConfig(2**64)),
        Config(["127.0.0.1"], 8000, node_list_polling=NodeListPollingConfig(True)),
        Config(["127.0.0.1"], 8000, retries=RetryConfig(max_attempts=2**32)),
        Config(["127.0.0.1"], 8000, retries=RetryConfig(max_attempts=True)),
        Config(
            ["127.0.0.1"],
            8000,
            request_compression=RequestCompressionConfig(
                min_size_bytes=2 * sys.maxsize + 2
            ),
        ),
        Config(
            ["127.0.0.1"],
            8000,
            request_compression=RequestCompressionConfig(min_size_bytes=True),
        ),
    ],
)
def test_native_integer_widths_and_types_rejected(config: Config) -> None:
    with pytest.raises(ConfigurationError):
        prepare_config(config)


def test_fallback_chain_above_old_cap_preserves_order() -> None:
    scope: RoutingScope = ClusterScope()
    expected: list[tuple[str, str | None, str | None]] = [("cluster", None, None)]
    for index in range(64):
        scope = DatacenterScope(f"dc-{index}", fallback=scope)
        expected.insert(0, ("datacenter", f"dc-{index}", None))
    prepared = prepare_config(Config(["127.0.0.1"], 8000, routing_scope=scope))
    assert prepared.native_config()["scopes"] == expected
    delegate_scope = prepared.delegate_config().routing_scope
    for kind, dc, _ in expected[:-1]:
        assert kind == "datacenter"
        assert isinstance(delegate_scope, DatacenterScope)
        assert delegate_scope.datacenter == dc
        assert delegate_scope.fallback is not None
        delegate_scope = delegate_scope.fallback
    assert isinstance(delegate_scope, ClusterScope)


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), 1e100, float(2**64), 1e-20, True]
)
def test_nonfinite_or_unrepresentable_timeouts(value: float) -> None:
    config = Config(["127.0.0.1"], 8000, timeouts=TimeoutConfig(read_seconds=value))
    with pytest.raises(ConfigurationError):
        prepare_config(config)


@pytest.mark.parametrize(
    "host",
    [
        "http://localhost",
        "localhost:8000",
        "[::1]",
        "bad host",
        "host/path",
        "host@name",
        "host\nname",
    ],
)
def test_host_only_seeds(host: str) -> None:
    with pytest.raises(ConfigurationError):
        prepare_config(Config([host], 8000))


def test_bare_ipv6_and_disabled_auth() -> None:
    mapping = prepare_config(Config(["::1"], 8000, user_agent=None)).native_config()
    assert mapping["seed_hosts"] == ["::1"]
    assert mapping["access_key_id"] is None
    assert mapping["user_agent"] is None


def test_ca_and_client_pem_contents_are_copied(tmp_path: Path) -> None:
    ca = tmp_path / "ca.pem"
    cert = tmp_path / "client.pem"
    key = tmp_path / "client.key"
    for path in [ca, cert, key]:
        path.write_bytes(path.name.encode())
    paths = [ca]
    prepared = prepare_config(
        Config(
            ["127.0.0.1"],
            8000,
            tls=TLS(
                custom_ca_cert_paths=paths, client_cert_path=cert, client_key_path=key
            ),
        )
    )
    ca.write_bytes(b"changed")
    paths.clear()
    assert prepared.native_config()["ca_pem"] == [b"ca.pem"]
    assert prepared.native_config()["client_cert_pem"] == b"client.pem"
    assert prepared.native_config()["client_key_pem"] == b"client.key"
    frozen_tls = prepared.delegate_config().tls
    assert frozen_tls.custom_ca_cert_paths[0].read_bytes() == b"ca.pem"
    assert frozen_tls.client_key_path is not None
    assert frozen_tls.client_key_path.read_bytes() == b"client.key"
    prepared.close()
    prepared.close()
    assert not frozen_tls.client_key_path.exists()


@pytest.mark.parametrize(
    "config",
    [
        Config(["127.0.0.1"], 8000, max_pool_connections=201),
        Config(["127.0.0.1"], 8000, parameter_validation=False),
        Config(["127.0.0.1"], 8000, max_decompressed_response_bytes=1024),
    ],
    ids=["pool-tuning", "validation-bypass", "response-decompression-limit"],
)
def test_unsupported_policy_does_not_invoke_user_callback(config: Config) -> None:
    calls: list[str] = []

    def user_agent(value: str) -> str:
        calls.append(value)
        return value

    with pytest.raises(ConfigurationError):
        prepare_config(replace(config, user_agent=user_agent))
    assert calls == []


def test_cyclic_scope_rejected() -> None:
    scope = DatacenterScope("dc")
    object.__setattr__(scope, "_fallback", scope)
    with pytest.raises(ConfigurationError, match="cyclic"):
        prepare_config(Config(["127.0.0.1"], 8000, routing_scope=scope))


def test_nested_config_subclass_rejected() -> None:
    class CustomTimeout(TimeoutConfig):
        pass

    with pytest.raises(ConfigurationError):
        prepare_config(replace(Config(["127.0.0.1"], 8000), timeouts=CustomTimeout()))
