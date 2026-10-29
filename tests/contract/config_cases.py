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

"""Native-only configuration rejection cases for later construction checks.

Python supports some of these settings. Do not impose native exclusions on the
existing Python factories. Each native case must fail before any discovery or
DynamoDB request, using alternator.exceptions.ConfigurationError.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from alternator import (
    TLS,
    ClusterScope,
    Config,
    ExponentialBackoff,
    HeaderOptimizationConfig,
    KeyRouteAffinityConfig,
    ResponseCompressionConfig,
    RetryConfig,
    RetryMode,
    TlsSessionCacheConfig,
)


class CustomScope(ClusterScope):
    """A custom policy remains unsupported even when it subclasses a built-in."""


@dataclass(frozen=True)
class RejectedConfig:
    """A valid Python configuration excluded from the native subset."""

    name: str
    config: Config
    exception: str = "alternator.exceptions.ConfigurationError"


def rejected_native_configs(base: Config) -> list[RejectedConfig]:
    """Return fresh valid Config instances with one unsupported setting each."""
    return [
        RejectedConfig("custom-routing", replace(base, routing_scope=CustomScope())),
        RejectedConfig(
            "header-whitelist",
            replace(
                base,
                header_optimization=HeaderOptimizationConfig(
                    enabled=True, whitelist=frozenset({"host"})
                ),
            ),
        ),
        RejectedConfig(
            "header-callback",
            replace(
                base,
                header_optimization=HeaderOptimizationConfig(
                    enabled=True, whitelist_callback=lambda ctx: ctx.required_headers
                ),
            ),
        ),
        RejectedConfig(
            "legacy-retries", replace(base, retries=RetryConfig(mode=RetryMode.LEGACY))
        ),
        RejectedConfig(
            "adaptive-retries",
            replace(base, retries=RetryConfig(mode=RetryMode.ADAPTIVE)),
        ),
        RejectedConfig("python-pool-tuning", replace(base, max_pool_connections=201)),
        RejectedConfig(
            "python-validation-bypass", replace(base, parameter_validation=False)
        ),
        RejectedConfig(
            "python-response-decompression-limit",
            replace(
                base,
                response_compression=ResponseCompressionConfig(
                    max_decompressed_bytes=1024
                ),
            ),
        ),
        RejectedConfig(
            "insecure-tls", replace(base, tls=TLS(trust_all_certificates=True))
        ),
        RejectedConfig(
            "hostname-verification", replace(base, tls=TLS(verify_hostname=False))
        ),
        RejectedConfig(
            "tls-key-log", replace(base, tls=TLS(key_log_file_path=Path("keys.log")))
        ),
        RejectedConfig(
            "tls-session-policy",
            replace(base, tls=TLS(session_cache=TlsSessionCacheConfig(enabled=False))),
        ),
        RejectedConfig(
            "custom-discovery-backoff",
            replace(
                base,
                key_affinity=KeyRouteAffinityConfig(
                    discovery_backoff=ExponentialBackoff(initial_seconds=2)
                ),
            ),
        ),
    ]
