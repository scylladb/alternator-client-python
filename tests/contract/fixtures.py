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

"""Run Python contracts by default; explicitly opt in to installed native backends."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

import pytest

from alternator import Config, NodeListPollingConfig, RetryConfig, TimeoutConfig
from tests.contract.backends import BackendFactory
from tests.fake_server import FakeAlternatorServer


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--contract-backend",
        action="append",
        default=[],
        choices=("python-sync", "python-async", "rust-sync", "rust-async"),
        help="Explicit compatibility backends (repeatable); default: both Python factories",
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "backend_factory" in metafunc.fixturenames:
        names = metafunc.config.getoption("contract_backend") or [
            "python-sync",
            "python-async",
        ]
        metafunc.parametrize(
            "backend_factory", [BackendFactory(name) for name in names], ids=names
        )


@pytest.fixture
def contract_config(fake_alternator_server: FakeAlternatorServer) -> Iterator[Config]:
    """Disable retries for error assertions and keep discovery on the fake peer."""
    endpoint = urlsplit(fake_alternator_server.url())
    fake_alternator_server.set_localnodes(["127.0.0.1"])
    fake_alternator_server.set_partition_key("contract", "pk")
    assert endpoint.port is not None
    yield Config(
        seed_hosts=["127.0.0.1"],
        port=endpoint.port,
        retries=RetryConfig(max_attempts=1),
        node_list_polling=NodeListPollingConfig(
            active_interval_ms=60000, idle_interval_ms=60000
        ),
        timeouts=TimeoutConfig(discovery_seconds=1, connect_seconds=1, read_seconds=2),
    )
    assert not [
        request
        for request in fake_alternator_server.captured_requests(operation="Scan")
        if request.table and request.table.startswith(".scylla.alternator.system.")
    ], "topology discovery must use GET /localnodes, never system-table Scan"


@pytest.fixture
def compatibility_manifest() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads(Path(__file__).with_name("manifest.json").read_text()),
    )
