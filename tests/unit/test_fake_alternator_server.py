"""Tests for the fake Alternator HTTP server fixture."""

from alternator._http import create_sync_http_fetcher
from tests.conftest import FakeAlternatorServer


def test_fake_server_serves_localnodes(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """The fake server can drive the real sync node fetcher."""
    fake_alternator_server.set_localnodes(["node2", "node1"])

    fetcher = create_sync_http_fetcher(timeout_seconds=1.0)
    nodes = fetcher(fake_alternator_server.url("/localnodes"))

    assert list(nodes) == ["node2", "node1"]
    assert fake_alternator_server.requested_paths() == ["/localnodes"]


def test_fake_server_serves_scoped_localnodes(
    fake_alternator_server: FakeAlternatorServer,
) -> None:
    """The fake server records scoped discovery queries."""
    fake_alternator_server.set_localnodes(["node1"], query="dc=dc1")

    fetcher = create_sync_http_fetcher(timeout_seconds=1.0)
    nodes = fetcher(fake_alternator_server.url("/localnodes?dc=dc1"))

    assert list(nodes) == ["node1"]
    assert fake_alternator_server.requested_queries() == ["dc=dc1"]
