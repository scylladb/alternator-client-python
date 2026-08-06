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

"""Shared constants for Alternator client modules."""

# Attribute names used to store manager and pk_cache references on boto3 clients
MANAGER_ATTR = "_alternator_manager"
MANAGER_OWNS_ATTR = "_alternator_manager_owns_lifecycle"
PK_CACHE_ATTR = "_alternator_pk_cache"

# Timeout in seconds for waiting on partition key discovery from concurrent requests
PK_DISCOVERY_TIMEOUT_SECONDS = 10.0
