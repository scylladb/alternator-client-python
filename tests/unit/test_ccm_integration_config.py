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

"""Focused tests for ordinary integration-suite CCM configuration."""

from __future__ import annotations

import os
import urllib.request

import pytest

from tests.integration import integration_tests_enabled
from tests.testinfra.cluster import add_no_proxy_hosts


@pytest.mark.parametrize(
    ("integration", "skip", "expected"),
    [
        (None, None, False),
        ("true", None, True),
        ("YES", "0", True),
        ("1", "true", False),
        ("0", "0", False),
    ],
)
def test_integration_enablement_honors_explicit_skip(
    monkeypatch: pytest.MonkeyPatch,
    integration: str | None,
    skip: str | None,
    expected: bool,
) -> None:
    for variable, value in (
        ("INTEGRATION_TESTS", integration),
        ("SKIP_INTEGRATION_TESTS", skip),
    ):
        if value is None:
            monkeypatch.delenv(variable, raising=False)
        else:
            monkeypatch.setenv(variable, value)

    assert integration_tests_enabled() is expected


def test_all_ccm_nodes_are_added_to_both_no_proxy_variants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NO_PROXY", "example.test,127.0.0.1")
    monkeypatch.setenv("no_proxy", "localhost")
    node_hosts = ("127.0.7.1", "127.0.7.2", "127.0.7.3")

    add_no_proxy_hosts(node_hosts)

    expected = [
        "example.test",
        "127.0.0.1",
        "localhost",
        *node_hosts,
    ]
    assert os.environ["NO_PROXY"].split(",") == expected
    assert os.environ["no_proxy"].split(",") == expected
    assert urllib.request.proxy_bypass("127.0.7.1")
    assert urllib.request.proxy_bypass("127.0.7.3")
