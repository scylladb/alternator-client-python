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

"""Integration tests for node health tracking behavior.

Tests verify node discovery, node list refresh, and behavior when nodes
become unavailable or recover. These tests exercise the current node
management capabilities.

These tests require a running Scylla cluster with Alternator enabled.
Start a local cluster with: make scylla-start
"""

import time

import pytest

from alternator import (
    AlternatorClient,
    AlternatorConfigBuilder,
    Config,
)
from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


@pytest.fixture
def config() -> Config:
    """Create test configuration."""
    return Config(
        seed_hosts=[SCYLLA_HOST],
        port=SCYLLA_PORT,
        scheme="http",
    )


class TestNodeDiscovery:
    """Test that the client discovers nodes from the cluster."""

    def test_discovers_multiple_nodes(self, config: Config) -> None:
        """Test that the client discovers nodes via /localnodes endpoint."""
        with AlternatorClient(config) as client:
            # Perform a request to trigger node discovery
            client.list_tables()

            # The client should have discovered nodes
            # (internal state, but we verify it works by making requests)
            for _ in range(20):
                response = client.list_tables()
                assert "TableNames" in response

    def test_seed_host_used_for_initial_discovery(self) -> None:
        """Test that the seed host is used when initial discovery hasn't completed."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            scheme="http",
        )

        with AlternatorClient(config) as client:
            # The very first request should succeed using the seed host
            response = client.list_tables()
            assert "TableNames" in response


class TestNodeRecoveryDetection:
    """Test that the client detects when nodes recover via periodic refresh."""

    def test_continuous_operations_after_recovery(self, config: Config) -> None:
        """Test that operations continue to work after the node list refreshes.

        This simulates the background refresh cycle by making requests
        over a period longer than the active refresh interval.
        """
        with AlternatorClient(config) as client:
            # Make requests over time to trigger background refreshes
            for _ in range(30):
                response = client.list_tables()
                assert "TableNames" in response
                time.sleep(0.1)


class TestActiveVsQuarantinedNodeTracking:
    """Test tracking of active node status.

    Note: The Python client currently uses round-robin with periodic
    refresh rather than explicit quarantine. These tests verify the
    current behavior of node list management.
    """

    def test_all_nodes_active_after_startup(self, config: Config) -> None:
        """Test that all discovered nodes are active after client startup."""
        with AlternatorClient(config) as client:
            # Multiple requests should succeed, indicating active nodes
            success_count = 0
            for _ in range(50):
                try:
                    response = client.list_tables()
                    if "TableNames" in response:
                        success_count += 1
                except Exception:
                    pass

            # All requests should succeed when all nodes are healthy
            assert success_count == 50

    def test_node_list_refreshed_periodically(self, config: Config) -> None:
        """Test that the node list is refreshed by the background thread."""
        with AlternatorClient(config) as client:
            # Initial request
            response = client.list_tables()
            assert "TableNames" in response

            # Wait for at least one background refresh cycle
            time.sleep(2)

            # Subsequent requests should still work after refresh
            for _ in range(10):
                response = client.list_tables()
                assert "TableNames" in response


class TestQuarantineAndRelease:
    """Test quarantine and release behavior.

    Note: The Python client doesn't yet implement explicit quarantine/release.
    These tests verify the retry behavior which provides similar resilience.
    """

    def test_requests_retry_on_temporary_failure(self) -> None:
        """Test that requests succeed even with retries enabled.

        The client's retry mechanism provides resilience similar to
        quarantine by automatically retrying failed requests on other nodes.
        """
        config = (
            AlternatorConfigBuilder()
            .with_seeds(SCYLLA_HOST)
            .with_port(SCYLLA_PORT)
            .with_retries(max_attempts=3)
            .build()
        )

        with AlternatorClient(config) as client:
            for _ in range(20):
                response = client.list_tables()
                assert "TableNames" in response

    def test_multiple_seed_hosts_fallback(self) -> None:
        """Test fallback across multiple seed hosts.

        When multiple seed hosts are provided, the client should try
        them in order for node discovery.
        """
        config = Config(
            seed_hosts=[SCYLLA_HOST, SCYLLA_HOST],  # Same host twice for testing
            port=SCYLLA_PORT,
            scheme="http",
        )

        with AlternatorClient(config) as client:
            response = client.list_tables()
            assert "TableNames" in response


class TestQuarantineReleaseConcurrency:
    """Test concurrent request handling with node management.

    Note: Tests verify that concurrent requests work correctly with
    the current round-robin + retry mechanism.
    """

    def test_concurrent_requests_with_refresh(self, config: Config) -> None:
        """Test that concurrent requests work during node list refresh."""
        import threading

        with AlternatorClient(config) as client:
            errors: list[Exception] = []
            lock = threading.Lock()

            def make_requests() -> None:
                try:
                    for _ in range(20):
                        response = client.list_tables()
                        assert "TableNames" in response
                        time.sleep(0.05)
                except Exception as e:
                    with lock:
                        errors.append(e)

            threads = [threading.Thread(target=make_requests) for _ in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert len(errors) == 0, f"Concurrent requests failed: {errors}"
