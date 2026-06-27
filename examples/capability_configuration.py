#!/usr/bin/env python3
"""
Configuration recipes for advanced Alternator client capabilities.

Running this file builds configuration objects only; it does not open network
connections. Copy the functions into application code as needed.
"""

from __future__ import annotations

from pathlib import Path

from alternator import (
    TLS,
    AlternatorConfigBuilder,
    Auth,
    ClusterScope,
    CompressionAlgorithm,
    Config,
    DatacenterScope,
    HeaderWhitelistContext,
    KeyRouteAffinityMode,
    RackScope,
    RetryMode,
    Session,
    TimeoutConfig,
)


def helper_lifecycle_example() -> None:
    """Use Session when one object should own clients and topology diagnostics."""
    config = Config(
        seed_hosts=["node1.example.com", "node2.example.com"],
        port=8000,
    )

    with Session(config, auth=Auth.disabled()) as helper:
        client = helper.client("dynamodb")
        resource = helper.resource("dynamodb")

        helper.refresh_nodes()
        print("nodes:", helper.nodes)

        client.list_tables()
        resource.Table("orders").scan(Limit=1)


def static_auth() -> Auth:
    """Use explicit static credentials when Alternator auth is enabled."""
    return Auth.static_credentials("alternator", "secret")


def explicit_routing_fallback_config() -> Config:
    """Route to a rack first, then its datacenter, then the full cluster."""
    return Config(
        seed_hosts=["node1.example.com", "node2.example.com"],
        port=8000,
        routing_scope=RackScope(
            "dc1",
            "rack1",
            fallback=DatacenterScope("dc1", fallback=ClusterScope()),
        ),
    )


def datacenter_only_config() -> Config:
    """Disable fallback when requests must stay inside one datacenter."""
    return Config(
        seed_hosts=["node1.example.com"],
        port=8000,
        routing_scope=DatacenterScope("dc1", fallback=None),
    )


def transport_config() -> Config:
    """Configure retries, per-attempt timeouts, pool size, and User-Agent."""
    return (
        AlternatorConfigBuilder()
        .with_seeds("node1.example.com", "node2.example.com")
        .with_port(8000)
        .with_retries(max_attempts=4, mode=RetryMode.STANDARD)
        .with_timeouts(
            discovery_seconds=3.0,
            connect_seconds=2.0,
            read_seconds=10.0,
        )
        .with_pool_connections(300)
        .with_aws_region("us-east-1")
        .with_user_agent(lambda default: f"orders-service {default}")
        .build()
    )


def mtls_config() -> Config:
    """Configure HTTPS discovery and SDK calls with client certificates."""
    tls = TLS(
        custom_ca_cert_paths=(Path("/etc/alternator/ca.pem"),),
        client_cert_path=Path("/etc/alternator/client.crt"),
        client_key_path=Path("/etc/alternator/client.key"),
        key_log_file_path=Path("/secure/tmp/alternator-tls.keys"),
    )
    return Config(
        seed_hosts=["node1.example.com"],
        port=8043,
        scheme="https",
        tls=tls,
    )


def extra_headers(context: HeaderWhitelistContext) -> set[str]:
    """Keep service headers that depend on auth and compression settings."""
    headers = {"X-Service-Trace"}
    if context.auth_enabled:
        headers.add("X-Auth-Trace")
    if context.compression_enabled:
        headers.add("X-Compression-Trace")
    return headers


def compression_and_header_config() -> Config:
    """Enable gzip request compression and dynamic header optimization."""
    return (
        AlternatorConfigBuilder()
        .with_seeds("node1.example.com")
        .with_port(8000)
        .with_compression(
            CompressionAlgorithm.GZIP,
            min_size=1024,
            gzip_level=6,
        )
        .with_header_optimization(
            whitelist={"X-Request-Id"},
            whitelist_callback=extra_headers,
        )
        .build()
    )


def key_affinity_config() -> Config:
    """Enable write affinity with preloaded partition-key metadata."""
    return (
        AlternatorConfigBuilder()
        .with_seeds("node1.example.com", "node2.example.com")
        .with_port(8000)
        .with_key_affinity(
            KeyRouteAffinityMode.ANY_WRITE,
            table_pk_map={"orders": "order_id", "customers": "customer_id"},
        )
        .build()
    )


def main() -> None:
    """Build example configs without connecting to Alternator."""
    configs = {
        "routing": explicit_routing_fallback_config(),
        "datacenter-only": datacenter_only_config(),
        "transport": transport_config(),
        "mtls": mtls_config(),
        "compression-headers": compression_and_header_config(),
        "key-affinity": key_affinity_config(),
    }
    auth = static_auth()

    for name, config in configs.items():
        print(f"{name}: seeds={list(config.seed_hosts)} port={config.port}")
    print("static auth enabled:", auth.enabled)
    print(
        "example timeout seconds:",
        TimeoutConfig().discovery_seconds,
        TimeoutConfig().connect_seconds,
        TimeoutConfig().read_seconds,
    )


if __name__ == "__main__":
    main()
