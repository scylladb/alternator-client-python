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

"""Public test-only API for native CCM clusters."""

from tests.testinfra.ccm_provisioner import (
    CcmClusterProvisioningError,
    CcmCommandError,
    CcmNodeProvisioningError,
    CcmProcessCleanupError,
    CcmProvisioner,
)
from tests.testinfra.cluster import (
    AlternatorConnection,
    PhysicalTestCluster,
    PrivateClusterControl,
    PrivateClusterLease,
    ReadOnlyTestCluster,
    ReusableClusterLease,
    TestClusterInfo,
    TestClusterNode,
    TestResourceScope,
)
from tests.testinfra.cluster_spec import (
    AlternatorTransport,
    AuthenticationMode,
    AuthorizationMode,
    ClusterSecuritySpec,
    ClusterSpec,
    ClusterSpecs,
    ClusterTopology,
    DatacenterSpec,
    NodeResources,
    RackSpec,
)
from tests.testinfra.pool import TestClusterPool, TestClusters
from tests.testinfra.run_state import CcmRunState, CcmRunStateError

__all__ = [
    "AlternatorConnection",
    "AlternatorTransport",
    "AuthenticationMode",
    "AuthorizationMode",
    "CcmClusterProvisioningError",
    "CcmCommandError",
    "CcmNodeProvisioningError",
    "CcmProcessCleanupError",
    "CcmProvisioner",
    "CcmRunState",
    "CcmRunStateError",
    "ClusterSecuritySpec",
    "ClusterSpec",
    "ClusterSpecs",
    "ClusterTopology",
    "DatacenterSpec",
    "NodeResources",
    "PhysicalTestCluster",
    "PrivateClusterControl",
    "PrivateClusterLease",
    "RackSpec",
    "ReadOnlyTestCluster",
    "ReusableClusterLease",
    "TestClusterInfo",
    "TestClusterNode",
    "TestClusterPool",
    "TestClusters",
    "TestResourceScope",
]
