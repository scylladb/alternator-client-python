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
