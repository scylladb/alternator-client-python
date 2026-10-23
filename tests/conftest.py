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

"""Shared pytest fixtures and configuration."""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING

import pytest

from tests.fake_server import (
    FakeAlternatorServer as FakeAlternatorServer,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests.integration.scylla_version import ScyllaVersion


@pytest.fixture
def fake_alternator_server() -> Iterator[FakeAlternatorServer]:
    """Provide a deterministic local HTTP server for Alternator unit tests."""
    server = FakeAlternatorServer()
    server.start()
    try:
        yield server
    finally:
        with contextlib.suppress(Exception):
            server.stop()


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires Scylla)"
    )
    config.addinivalue_line(
        "markers",
        "min_scylla_version(version): skip test if ScyllaDB version is below minimum",
    )


@pytest.fixture(scope="session")
def scylla_version() -> ScyllaVersion | None:
    """Get the running ScyllaDB version.

    This fixture attempts to detect the ScyllaDB version in the following order:
    1. SCYLLA_VERSION environment variable (e.g., "2026.1.0")
    2. Auto-detection from the running cluster

    Returns:
        ScyllaVersion object or None if version cannot be determined

    Usage:
        def test_something(scylla_version):
            if scylla_version and scylla_version >= ScyllaVersion(2026, 1, 0):
                # Test compression feature
                ...
    """
    # Import here to avoid issues when running unit tests only
    try:
        from tests.integration.scylla_version import (
            clear_version_cache,
            get_version_from_env,
        )
    except ImportError:
        return None

    # Clear any cached version from previous runs
    clear_version_cache()

    # First try environment variable
    env_version = get_version_from_env()
    if env_version:
        return env_version

    # Try auto-detection with a temporary client
    from tests.integration import SCYLLA_HOST, SCYLLA_PORT, SKIP_INTEGRATION

    if SKIP_INTEGRATION:
        return None

    try:
        from alternator import Config, close_client, create_client
        from tests.integration.scylla_version import detect_version_from_cluster

        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_PORT,
            scheme="http",
        )
        client = create_client(config)
        try:
            version = detect_version_from_cluster(client)
            return version
        finally:
            close_client(client)
    except Exception:
        return None


@pytest.fixture
def skip_if_scylla_version_below(
    scylla_version: ScyllaVersion | None,
) -> Callable[..., None]:
    """Factory fixture to skip tests based on ScyllaDB version.

    Usage:
        def test_compression(skip_if_scylla_version_below):
            skip_if_scylla_version_below(ScyllaVersion(2026, 1, 0), "gzip compression")
            # Test code here...
    """
    from tests.integration.scylla_version import (
        ScyllaVersion,  # noqa: F401 - used in type annotation below
        requires_scylla_version,
    )

    def _skip_if_below(min_version: ScyllaVersion, feature_name: str = "") -> None:
        if scylla_version is None:
            pytest.skip(
                f"ScyllaDB version unknown - set SCYLLA_VERSION env var. "
                f"{requires_scylla_version(min_version, feature_name)}"
            )
        if scylla_version < min_version:
            pytest.skip(
                f"ScyllaDB {scylla_version} < {min_version}. "
                f"{requires_scylla_version(min_version, feature_name)}"
            )

    return _skip_if_below
