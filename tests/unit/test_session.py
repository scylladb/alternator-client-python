"""Tests for public Session facades."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import urlsplit

import pytest

import alternator
from alternator import Auth, Config, Session
from alternator._constants import MANAGER_ATTR, MANAGER_OWNS_ATTR
from alternator.async_client import AsyncSession
from alternator.client import _close_client
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


def test_session_is_exported() -> None:
    """Session is available from the top-level package."""
    assert alternator.Session is Session
    assert alternator.AsyncSession is AsyncSession


def test_session_accepts_seed_keywords() -> None:
    """Session can be configured directly without a prebuilt Config."""
    session = Session(seeds=["node1", "node2"], port=8042, scheme="https")

    assert session.config.seed_hosts == ("node1", "node2")
    assert session.config.port == 8042
    assert session.config.scheme == "https"


def test_session_lifecycle_and_node_diagnostics(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """Session exposes explicit lifecycle and live-node inspection."""
    fake_alternator_server.set_localnodes(["node2", "node1"])
    session = Session(_config_for_server(fake_alternator_server))

    assert session.nodes == []
    assert session.refresh_nodes() is True
    assert session.nodes == ["node1", "node2"]
    assert session.active_nodes == ["node1", "node2"]
    assert session.quarantined_nodes == []

    session.start()
    session.start()
    session.stop()
    session.stop()
    assert session.nodes == []


def test_session_created_clients_borrow_session_manager(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """Closing a session-created client does not stop the session manager."""
    fake_alternator_server.set_localnodes(["node1"])

    with Session(_config_for_server(fake_alternator_server)) as session:
        client = session.client("dynamodb")
        resource = session.resource("dynamodb")

        assert getattr(client, MANAGER_ATTR) is session._manager
        assert getattr(resource, MANAGER_ATTR) is session._manager
        assert getattr(client, MANAGER_OWNS_ATTR) is False
        assert getattr(resource, MANAGER_OWNS_ATTR) is False

        _close_client(client)
        assert session.refresh_nodes() is True


def test_session_update_returns_new_session(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """update returns a fresh session with merged settings."""
    config = _config_for_server(fake_alternator_server)
    session = Session(config, auth=Auth.disabled(), region_name="us-west-2")
    updated = session.update(region_name="us-east-1")

    assert updated is not session
    assert updated.config is config
    assert updated._auth == Auth.disabled()
    assert updated._boto_kwargs["region_name"] == "us-east-1"


def test_session_topology_checks(fake_alternator_server: FakeAlternatorServer) -> None:
    """Session exposes lightweight topology configuration checks."""
    fake_alternator_server.set_localnodes(["node1"], query="dc=dc1")
    fake_alternator_server.set_localnodes(["node1"], query="dc=dc1&rack=rack1")

    assert Session(
        _config_for_server(fake_alternator_server, routing_scope=ClusterScope())
    ).validate_scope()
    assert Session(
        _config_for_server(
            fake_alternator_server,
            routing_scope=DatacenterScope(datacenter="dc1"),
        )
    ).validate_scope()

    invalid_session = Session(
        _config_for_server(
            fake_alternator_server,
            routing_scope=RackScope(datacenter="dc1", rack=""),
        )
    )
    assert not invalid_session.supports_topology_filters()
    with pytest.raises(ConfigurationError, match="non-empty"):
        invalid_session.validate_scope()

    assert Session(
        _config_for_server(
            fake_alternator_server,
            routing_scope=RackScope(datacenter="dc1", rack="rack1"),
        )
    ).supports_topology_filters()


def test_session_partition_key_diagnostics_from_config(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """Configured partition-key mappings are exposed without private access."""
    session = Session(
        _config_for_server(
            fake_alternator_server,
            key_affinity=KeyRouteAffinityConfig(table_pk_attributes={"tbl": "pk"}),
        )
    )

    assert session.partition_key_for("tbl") == "pk"
    assert session.partition_key_for("missing") is None


def test_no_fallback_scope_does_not_use_seed_hosts(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """Explicit no-fallback scopes stay constrained when discovery is empty."""
    config = _config_for_server(
        fake_alternator_server,
        routing_scope=DatacenterScope("missing", fallback=None),
    )

    with (
        pytest.raises(NoNodesAvailableError),
        alternator.client("dynamodb", cluster_config=config),
    ):
        pass


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
    with alternator.client("dynamodb", cluster_config=config) as client:
        assert client.meta.region_name == "us-west-2"


def test_close_client_closes_underlying_boto_client() -> None:
    """The internal close helper releases the botocore HTTP session."""
    manager = SimpleNamespace(stop=Mock())
    client = SimpleNamespace(close=Mock())
    setattr(client, MANAGER_ATTR, manager)
    setattr(client, MANAGER_OWNS_ATTR, True)

    _close_client(client)  # type: ignore[arg-type] # lightweight boto client stub
    _close_client(client)  # type: ignore[arg-type] # idempotency check

    assert manager.stop.call_count == 1
    assert client.close.call_count == 2


def test_close_resource_closes_underlying_boto_client() -> None:
    """The internal close helper releases the resource's botocore client."""
    manager = SimpleNamespace(stop=Mock())
    service_client = SimpleNamespace(close=Mock())
    resource = SimpleNamespace(meta=SimpleNamespace(client=service_client))
    setattr(resource, MANAGER_ATTR, manager)
    setattr(resource, MANAGER_OWNS_ATTR, True)

    _close_client(resource)  # type: ignore[arg-type] # lightweight resource stub

    manager.stop.assert_called_once_with()
    service_client.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_session_accepts_seed_keywords() -> None:
    """AsyncSession can be configured directly without a prebuilt Config."""
    session = AsyncSession(seeds=["node1", "node2"], port=8042, scheme="https")

    assert session.config.seed_hosts == ("node1", "node2")
    assert session.config.port == 8042
    assert session.config.scheme == "https"


@pytest.mark.asyncio
async def test_async_session_rejects_unsupported_service_name() -> None:
    """AsyncSession only creates DynamoDB clients."""
    session = AsyncSession(seeds=["node1"], port=8000)

    with pytest.raises(ConfigurationError, match="'dynamodb'"):
        await session.client("s3")


@pytest.mark.asyncio
async def test_async_session_lifecycle_and_node_diagnostics(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """AsyncSession exposes async lifecycle and live-node inspection."""
    fake_alternator_server.set_localnodes(["node2", "node1"])
    session = AsyncSession(_config_for_server(fake_alternator_server))

    assert session.nodes == []
    assert await session.refresh_nodes() is True
    assert session.nodes == ["node1", "node2"]
    assert session.active_nodes == ["node1", "node2"]
    assert session.quarantined_nodes == []

    await session.start()
    await session.start()
    await session.stop()
    await session.stop()
    assert session.nodes == []
