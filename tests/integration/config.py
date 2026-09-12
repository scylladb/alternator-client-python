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

"""CCM-derived connection settings shared by ordinary integration tests."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit

from tests.integration import integration_tests_enabled
from tests.testinfra.cluster import add_no_proxy_hosts
from tests.testinfra.cluster_spec import AlternatorTransport, ClusterSpecs
from tests.testinfra.pool import TestClusters

INTEGRATION_ENABLED = integration_tests_enabled()
SKIP_INTEGRATION = not INTEGRATION_ENABLED

if not SKIP_INTEGRATION:
    _SUITE_CLUSTER = TestClusters.acquire_reusable(ClusterSpecs.default_spec())
    add_no_proxy_hosts(node.address for node in _SUITE_CLUSTER.cluster.nodes)
    _http = _SUITE_CLUSTER.cluster.connection(AlternatorTransport.HTTP)
    _https = _SUITE_CLUSTER.cluster.connection(AlternatorTransport.HTTPS)
    _http_endpoint = urlsplit(_http.seed_endpoint)
    _https_endpoint = urlsplit(_https.seed_endpoint)
    if _http_endpoint.hostname is None or _https_endpoint.hostname is None:
        raise RuntimeError("CCM returned an invalid Alternator seed endpoint")
    SCYLLA_HOST = _http_endpoint.hostname
    SCYLLA_PORT = _http_endpoint.port or 8080
    SCYLLA_HTTPS_PORT = _https_endpoint.port or 8043
    SCYLLA_DATACENTER = _SUITE_CLUSTER.cluster.nodes[0].datacenter
    SCYLLA_RACK = _SUITE_CLUSTER.cluster.nodes[0].rack
    SCYLLA_CA_CERT_PATH = _https.ca_certificate_path
else:
    _SUITE_CLUSTER = None
    SCYLLA_HOST = "127.0.0.1"
    SCYLLA_PORT = 8080
    SCYLLA_HTTPS_PORT = 8043
    SCYLLA_DATACENTER = "dc1"
    SCYLLA_RACK = "RAC1"
    SCYLLA_CA_CERT_PATH: Path | None = None


def new_table_name(hint: str) -> str:
    """Create a table name owned by the suite cluster lease."""
    if _SUITE_CLUSTER is None:
        raise RuntimeError("Integration-test CCM cluster is not enabled")
    return _SUITE_CLUSTER.resources.new_table_name(hint)
