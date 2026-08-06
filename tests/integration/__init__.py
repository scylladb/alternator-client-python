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

"""Integration tests for alternator package."""

import os

SCYLLA_HOST = os.environ.get("SCYLLA_HOST", "localhost")
SCYLLA_PORT = int(os.environ.get("SCYLLA_PORT", "9998"))
SCYLLA_HTTPS_PORT = int(os.environ.get("SCYLLA_HTTPS_PORT", "9999"))
SKIP_INTEGRATION = os.environ.get("SKIP_INTEGRATION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
)
