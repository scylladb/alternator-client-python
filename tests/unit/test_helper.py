"""Tests for public Helper facades."""

from __future__ import annotations

from urllib.parse import urlsplit

import pytest

import alternator
from alternator import Auth, Config, Helper, close_client, create_client
from alternator._constants import MANAGER_ATTR, MANAGER_OWNS_ATTR
from alternator.async_client import AsyncHelper
from alternator.config import KeyRouteAffinityConfig
from alternator.core.routing_scope import (
    ClusterScope,
    DatacenterScope,
    RackScope,
    RoutingScope,
)
from alternator.exceptions import ConfigurationError, NoNodesAvailableError
from tests.conftest import FakeAlternatorServer


def _config_for_server(
    server: FakeAlternatorServer,
    *,
    routing_scope: RoutingScope | None = None,
    key_affinity: KeyRouteAffinityConfig | None = None,
) -> Config:
    parsed = urlsplit(server.url("/"))
    assert parsed.hostname is not None
    assert parsed.port is not None

    return Config(
        seed_hosts=[parsed.hostname],
        port=parsed.port,
        routing_scope=routing_scope or ClusterScope(),
        key_affinity=key_affinity or KeyRouteAffinityConfig(),
    )


def test_helper_is_exported() -> None:
    """Helper is available from the top-level package."""
    assert alternator.Helper is Helper
    assert alternator.AsyncHelper is AsyncHelper


def test_helper_lifecycle_and_node_diagnostics(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """Helper exposes explicit lifecycle and live-node inspection."""
    fake_alternator_server.set_localnodes(["node2", "node1"])
    helper = Helper(_config_for_server(fake_alternator_server))

    assert helper.get_nodes() == []
    assert helper.update_live_nodes() is True
    assert helper.get_nodes() == ["node1", "node2"]
    assert helper.get_active_nodes() == ["node1", "node2"]
    assert helper.get_quarantined_nodes() == []
    assert helper.next_node() == "node1"
    assert helper.next_node() == "node2"

    helper.start()
    helper.start()
    helper.stop()
    helper.stop()
    assert helper.get_nodes() == []


def test_helper_created_clients_borrow_helper_manager(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """Closing a helper-created client does not stop the helper manager."""
    fake_alternator_server.set_localnodes(["node1"])

    with Helper(_config_for_server(fake_alternator_server)) as helper:
        client = helper.client()
        resource = helper.resource()

        assert getattr(client, MANAGER_ATTR) is helper._manager
        assert getattr(resource, MANAGER_ATTR) is helper._manager
        assert getattr(client, MANAGER_OWNS_ATTR) is False
        assert getattr(resource, MANAGER_OWNS_ATTR) is False

        close_client(client)
        assert helper.update_live_nodes() is True


def test_helper_update_returns_new_helper(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """update returns a fresh helper with merged settings."""
    config = _config_for_server(fake_alternator_server)
    helper = Helper(config, auth=Auth.disabled(), region_name="us-west-2")
    updated = helper.update(region_name="us-east-1")

    assert updated is not helper
    assert updated.config is config
    assert updated._auth == Auth.disabled()
    assert updated._boto_kwargs["region_name"] == "us-east-1"


def test_helper_topology_checks(fake_alternator_server: FakeAlternatorServer) -> None:
    """Helper exposes lightweight topology configuration checks."""
    fake_alternator_server.set_localnodes(["node1"], query="dc=dc1")
    fake_alternator_server.set_localnodes(["node1"], query="dc=dc1&rack=rack1")

    assert Helper(
        _config_for_server(fake_alternator_server, routing_scope=ClusterScope())
    ).check_rack_and_datacenter_set_correctly()
    assert Helper(
        _config_for_server(
            fake_alternator_server,
            routing_scope=DatacenterScope(datacenter="dc1"),
        )
    ).check_rack_and_datacenter_set_correctly()

    invalid_helper = Helper(
        _config_for_server(
            fake_alternator_server,
            routing_scope=RackScope(datacenter="dc1", rack=""),
        )
    )
    assert not invalid_helper.check_rack_datacenter_feature_supported()
    with pytest.raises(ConfigurationError, match="non-empty"):
        invalid_helper.check_rack_and_datacenter_set_correctly()

    assert Helper(
        _config_for_server(
            fake_alternator_server,
            routing_scope=RackScope(datacenter="dc1", rack="rack1"),
        )
    ).check_rack_datacenter_feature_supported()


def test_helper_partition_key_diagnostics_from_config(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """Configured partition-key mappings are exposed without private access."""
    helper = Helper(
        _config_for_server(
            fake_alternator_server,
            key_affinity=KeyRouteAffinityConfig(table_pk_attributes={"tbl": "pk"}),
        )
    )

    assert helper.get_partition_key_name("tbl") == "pk"
    assert helper.get_partition_key_name("missing") is None


def test_no_fallback_scope_does_not_use_seed_hosts(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """Explicit no-fallback scopes stay constrained when discovery is empty."""
    config = _config_for_server(
        fake_alternator_server,
        routing_scope=DatacenterScope("missing", fallback=None),
    )

    with pytest.raises(NoNodesAvailableError):
        create_client(config)


def test_create_client_uses_configured_aws_region(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """Config aws_region is passed to the generated boto3 client."""
    fake_alternator_server.set_localnodes(["node1"])
    base_config = _config_for_server(fake_alternator_server)
    config = Config(
        seed_hosts=base_config.seed_hosts,
        port=base_config.port,
        aws_region="us-west-2",
    )
    client = create_client(config)
    try:
        assert client.meta.region_name == "us-west-2"
    finally:
        close_client(client)


@pytest.mark.asyncio
async def test_async_helper_lifecycle_and_node_diagnostics(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """AsyncHelper exposes async lifecycle and live-node inspection."""
    fake_alternator_server.set_localnodes(["node2", "node1"])
    helper = AsyncHelper(_config_for_server(fake_alternator_server))

    assert helper.get_nodes() == []
    assert await helper.update_live_nodes() is True
    assert helper.get_nodes() == ["node1", "node2"]
    assert helper.get_active_nodes() == ["node1", "node2"]
    assert helper.get_quarantined_nodes() == []
    assert await helper.next_node() == "node1"

    await helper.start()
    await helper.start()
    await helper.stop()
    await helper.stop()
    assert helper.get_nodes() == []
