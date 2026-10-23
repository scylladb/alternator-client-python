# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Validate and freeze the supported native configuration before loading Rust.

P8 consumes ``prepare_config`` and passes ``prepared.native_config()`` to the
extension. Its lazy Python admin factory uses ``prepared.delegate_config()`` and
``prepared.auth``. Each method returns a fresh owned snapshot; callbacks never
reach either client.
"""

from __future__ import annotations

import ipaddress
import math
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType
from typing import Any

from alternator.backoff import ExponentialBackoff
from alternator.config import (
    TLS,
    AlternatorConfig,
    Auth,
    CompressionAlgorithm,
    Config,
    HeaderOptimizationConfig,
    KeyRouteAffinityConfig,
    KeyRouteAffinityMode,
    NodeListPollingConfig,
    RequestCompressionConfig,
    ResponseCompression,
    RetryConfig,
    RetryMode,
    TimeoutConfig,
    TlsConfig,
    TlsSessionCacheConfig,
    _resolve_user_agent,
)
from alternator.core.routing_scope import (
    ClusterScope,
    DatacenterScope,
    RackScope,
    RoutingScope,
)
from alternator.exceptions import ConfigurationError

_MAX_U64 = 2**64 - 1
_MAX_U32 = 2**32 - 1
_MAX_USIZE = sys.maxsize * 2 + 1


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ConfigurationError(message)


def _string(value: object, name: str) -> str:
    _require(type(value) is str and bool(value), f"{name} must be a non-empty string")
    assert isinstance(value, str)
    _require(
        not any(ord(char) < 32 or ord(char) == 127 for char in value), f"invalid {name}"
    )
    return value


def _integer(value: object, name: str, minimum: int, maximum: int) -> int:
    _require(
        type(value) is int and minimum <= value <= maximum,
        f"{name} must be an integer in {minimum}..{maximum}",
    )
    assert isinstance(value, int)
    return value


def _seconds(value: object, name: str) -> float:
    _require(type(value) in (int, float), f"{name} must be finite positive seconds")
    try:
        result = float(value)  # type: ignore[arg-type]
    except (OverflowError, ValueError) as error:
        raise ConfigurationError(f"{name} is not representable") from error
    _require(
        math.isfinite(result) and 0 < result < 2**64,
        f"{name} must be finite positive representable seconds",
    )
    _require(result >= 1e-9, f"{name} must be at least one nanosecond")
    # Rust also checks Duration conversion and the platform's monotonic deadline.
    return result


def _scope(
    scope: RoutingScope,
) -> tuple[RoutingScope, tuple[tuple[str, str | None, str | None], ...]]:
    seen: set[int] = set()
    chain: list[tuple[str, str | None, str | None]] = []
    current: RoutingScope | None = scope
    while current is not None:
        _require(id(current) not in seen, "cyclic routing fallback")
        seen.add(id(current))
        if type(current) is ClusterScope:
            chain.append(("cluster", None, None))
        elif type(current) is DatacenterScope:
            chain.append(
                ("datacenter", _string(current.datacenter, "datacenter"), None)
            )
        elif type(current) is RackScope:
            chain.append(
                (
                    "rack",
                    _string(current.datacenter, "datacenter"),
                    _string(current.rack, "rack"),
                )
            )
        else:
            raise ConfigurationError(
                "native backend requires exact built-in routing scopes"
            )
        current = current.fallback
    frozen: RoutingScope | None = None
    for kind, dc, rack in reversed(chain):
        if kind == "cluster":
            frozen = ClusterScope()
        elif kind == "datacenter":
            assert dc is not None
            frozen = DatacenterScope(dc, fallback=frozen)
        else:
            assert dc is not None and rack is not None
            frozen = RackScope(dc, rack, fallback=frozen)
    assert frozen is not None
    return frozen, tuple(chain)


def _pem(path: Path, name: str) -> bytes:
    _require(isinstance(path, Path), f"{name} must be a pathlib.Path")
    try:
        return path.read_bytes()
    except OSError as error:
        raise ConfigurationError(f"cannot read {name}: {path}") from error


@dataclass(frozen=True)
class PreparedConfig:
    """Owned config snapshots, with construction-time callbacks already resolved."""

    _config: Config
    auth: Auth
    _scopes: tuple[tuple[str, str | None, str | None], ...]
    _ca_pem: tuple[bytes, ...]
    _cert_pem: bytes | None
    _key_pem: bytes | None
    _tls_files: TemporaryDirectory[str] | None

    def close(self) -> None:
        """Release frozen delegate PEM files after closing the Python delegate."""
        if self._tls_files is not None:
            self._tls_files.cleanup()

    def delegate_config(self) -> Config:
        """Return a fresh built-in Config for the lazy Python admin delegate."""
        return replace(
            self._config,
            key_affinity=replace(
                self._config.key_affinity,
                table_pk_attributes=dict(self._config.key_affinity.table_pk_attributes),
            ),
        )

    def native_config(self) -> dict[str, Any]:
        """Return the v1 owned Python-to-Rust mapping (no user callbacks)."""
        config = self._config
        return {
            "schema_version": 1,
            "seed_hosts": list(config.seed_hosts),
            "port": config.port,
            "scheme": config.scheme,
            "scopes": list(self._scopes),
            "active_interval_ms": config.node_list_polling.active_interval_ms,
            "idle_interval_ms": config.node_list_polling.idle_interval_ms,
            "key_affinity": config.key_affinity.mode.name,
            "table_pk_attributes": dict(config.key_affinity.table_pk_attributes),
            "access_key_id": self.auth.access_key_id,
            "secret_access_key": self.auth.secret_access_key,
            "session_token": self.auth.session_token,
            "region": config.aws_region,
            "request_gzip": config.request_compression.algorithm
            is CompressionAlgorithm.GZIP,
            "compression_min_bytes": config.request_compression.min_size_bytes,
            "gzip_level": config.request_compression.gzip_level,
            "response_compression": [
                value.value for value in config.response_compression
            ],
            "optimize_headers": config.header_optimization.enabled,
            "user_agent": config.user_agent,
            "max_attempts": config.retries.max_attempts,
            "connect_seconds": config.timeouts.connect_seconds,
            "read_seconds": config.timeouts.read_seconds,
            "discovery_seconds": config.timeouts.discovery_seconds,
            "system_roots": config.tls.trust_system_ca_certs,
            "ca_pem": list(self._ca_pem),
            "client_cert_pem": self._cert_pem,
            "client_key_pem": self._key_pem,
        }


def prepare_config(config: Config, auth: Auth | None = None) -> PreparedConfig:
    """Reject unsupported settings before any native construction or network I/O."""
    _require(
        type(config) in (Config, AlternatorConfig),
        "native backend requires built-in Config",
    )
    auth = Auth.disabled() if auth is None else auth
    _require(type(auth) is Auth, "native backend requires built-in Auth")
    for value, expected, name in (
        (config.request_compression, RequestCompressionConfig, "request_compression"),
        (config.header_optimization, HeaderOptimizationConfig, "header_optimization"),
        (config.key_affinity, KeyRouteAffinityConfig, "key_affinity"),
        (config.retries, RetryConfig, "retries"),
        (config.timeouts, TimeoutConfig, "timeouts"),
        (config.node_list_polling, NodeListPollingConfig, "node_list_polling"),
    ):
        _require(type(value) is expected, f"native backend requires built-in {name}")
    _require(
        type(config.tls) in (TLS, TlsConfig), "native backend requires built-in TLS"
    )
    _require(
        type(config.tls.session_cache) is TlsSessionCacheConfig
        and config.tls.session_cache == TlsSessionCacheConfig(),
        "native backend does not support custom TLS session settings",
    )
    _require(
        config.tls.trust_all_certificates is False
        and config.tls.verify_hostname is True
        and config.tls.key_log_file_path is None,
        "native backend requires verified TLS without key logging",
    )
    _require(
        type(config.tls.trust_system_ca_certs) is bool,
        "trust_system_ca_certs must be bool",
    )
    _require(
        config.max_pool_connections == 200 and type(config.max_pool_connections) is int,
        "native backend does not support Python pool tuning",
    )
    _require(
        config.parameter_validation is True,
        "native backend requires parameter_validation=True",
    )
    _require(
        config.retries.mode is RetryMode.STANDARD,
        "native backend supports STANDARD retries only",
    )
    _integer(config.retries.max_attempts, "max_attempts", 1, _MAX_U32)
    _require(
        type(config.header_optimization.enabled) is bool,
        "header optimization enabled must be bool",
    )
    _require(
        config.header_optimization.whitelist is None
        and config.header_optimization.whitelist_callback is None,
        "native backend does not support custom header policies",
    )
    _require(
        type(config.key_affinity.discovery_backoff) is ExponentialBackoff
        and config.key_affinity.discovery_backoff == ExponentialBackoff(),
        "native backend does not support custom discovery backoff",
    )
    _require(
        type(config.key_affinity.mode) is KeyRouteAffinityMode,
        "invalid key affinity mode",
    )
    _require(
        type(config.request_compression.algorithm) is CompressionAlgorithm,
        "invalid request compression algorithm",
    )
    _integer(
        config.request_compression.min_size_bytes,
        "compression threshold",
        0,
        _MAX_USIZE,
    )
    _integer(config.request_compression.gzip_level, "gzip_level", 0, 9)
    _integer(config.port, "port", 1, 65535)
    _require(config.scheme in ("http", "https"), "invalid scheme")
    _string(config.aws_region, "aws_region")
    seeds = tuple(config.seed_hosts)
    _require(bool(seeds), "at least one seed is required")
    for host in seeds:
        _string(host, "seed host")
        _require(
            not any(char in host for char in "/\\?#@[]")
            and not any(char.isspace() for char in host),
            "seed hosts must be host-only names or bare IP addresses",
        )
        if ":" in host:
            try:
                ipaddress.IPv6Address(host)
            except ValueError as error:
                raise ConfigurationError(
                    "seed host contains a port or invalid IPv6 address"
                ) from error
    scope, scopes = _scope(config.routing_scope)
    _integer(
        config.node_list_polling.active_interval_ms, "active_interval_ms", 1, _MAX_U64
    )
    _integer(config.node_list_polling.idle_interval_ms, "idle_interval_ms", 1, _MAX_U64)
    timeouts = TimeoutConfig(
        **{
            name: _seconds(getattr(config.timeouts, name), name)
            for name in ("connect_seconds", "read_seconds", "discovery_seconds")
        }
    )
    for table, key in config.key_affinity.table_pk_attributes.items():
        _string(table, "table name")
        _require(
            type(key) is str and bool(key), "partition key must be a non-empty string"
        )
    for encoding in config.response_compression:
        _require(type(encoding) is ResponseCompression, "invalid response compression")
    if auth.enabled:
        _string(auth.access_key_id, "access key")
        _string(auth.secret_access_key, "secret key")
        if auth.session_token is not None:
            _string(auth.session_token, "session token")
    elif auth.secret_access_key is not None or auth.session_token is not None:
        raise ConfigurationError("invalid disabled auth")
    _require(
        config.user_agent is None
        or isinstance(config.user_agent, str)
        or callable(config.user_agent),
        "user_agent must be a string, None or construction callback",
    )
    # Resolve the only supported callback after every unsupported setting check.
    user_agent = (
        None if config.user_agent is None else _resolve_user_agent(config.user_agent)
    )
    if user_agent is not None:
        _string(user_agent, "user_agent")
        _require(user_agent.isascii(), "user_agent must contain HTTP ASCII characters")
    ca_paths = tuple(config.tls.custom_ca_cert_paths)
    cert = config.tls.client_cert_path
    key_path = config.tls.client_key_path
    ca_pem = tuple(_pem(path, "CA certificate") for path in ca_paths)
    cert_pem = None if cert is None else _pem(cert, "client certificate")
    key_pem = None if key_path is None else _pem(key_path, "client key")
    # The lazy delegate must consume the same certificate bytes even if the
    # original files change. Private temporary files live until facade close.
    tls_files: TemporaryDirectory[str] | None = None
    if ca_pem or cert_pem is not None:
        try:
            tls_files = TemporaryDirectory(prefix="alternator-native-tls-")
            directory = Path(tls_files.name)
            frozen_paths = []
            for index, pem in enumerate(ca_pem):
                path = directory / f"ca-{index}.pem"
                path.write_bytes(pem)
                path.chmod(0o600)
                frozen_paths.append(path)
            ca_paths = tuple(frozen_paths)
            if cert_pem is not None:
                cert = directory / "client.pem"
                cert.write_bytes(cert_pem)
                cert.chmod(0o600)
            if key_pem is not None:
                key_path = directory / "client.key"
                key_path.write_bytes(key_pem)
                key_path.chmod(0o600)
        except OSError as error:
            if tls_files is not None:
                tls_files.cleanup()
            raise ConfigurationError(
                "cannot freeze native TLS certificate files"
            ) from error
    frozen = Config(
        seed_hosts=seeds,
        port=config.port,
        scheme=config.scheme,
        routing_scope=scope,
        request_compression=replace(config.request_compression),
        response_compression=tuple(config.response_compression),
        header_optimization=HeaderOptimizationConfig(
            enabled=config.header_optimization.enabled
        ),
        tls=TLS(
            custom_ca_cert_paths=ca_paths,
            trust_system_ca_certs=config.tls.trust_system_ca_certs,
            client_cert_path=cert,
            client_key_path=key_path,
        ),
        key_affinity=KeyRouteAffinityConfig(
            mode=config.key_affinity.mode,
            table_pk_attributes=dict(config.key_affinity.table_pk_attributes),
        ),
        retries=replace(config.retries),
        node_list_polling=replace(config.node_list_polling),
        timeouts=timeouts,
        parameter_validation=config.parameter_validation,
        aws_region=config.aws_region,
        user_agent=user_agent,
    )
    object.__setattr__(
        frozen.key_affinity,
        "table_pk_attributes",
        MappingProxyType(dict(frozen.key_affinity.table_pk_attributes)),
    )
    return PreparedConfig(
        frozen, replace(auth), scopes, ca_pem, cert_pem, key_pem, tls_files
    )
