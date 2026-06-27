# Alternator Client Capability Matrix

This matrix tracks current Python Alternator client capabilities, planned work,
and intentionally deferred behavior.

| Capability | Status | Tracking | Notes |
| --- | --- | --- | --- |
| Sync DynamoDB client | Supported | Existing API | `create_client`, `AlternatorClient`, and `alternator.client(...)` return standard boto3 clients. |
| DynamoDB resource | Supported | Existing API | `create_resource` and `AlternatorResource` wrap boto3 resource usage. |
| Async DynamoDB client | Supported | Existing API | `create_async_client` and `AsyncAlternatorClient` use aioboto3. |
| Host-only seeds with one shared port | Supported | Existing API | Seeds must not include ports; one port applies to all nodes. |
| Node discovery | Supported | Existing API | `/localnodes` refresh updates the live node list. `ClusterScope` combines results from all configured seeds, so multi-DC routing requires at least one reachable seed from each datacenter. |
| Routing scopes | Supported | [#35](https://github.com/scylladb/alternator-client-python/issues/35) | Cluster, datacenter, and rack scopes support explicit fallback chains and scoped validation helpers. |
| Request query plans | Supported | Existing API | Requests use stable seeded node ordering for retries. |
| Auth | Supported | Existing API | Disabled by default; explicit static credentials enable signing. |
| Request compression | Supported | [#37](https://github.com/scylladb/alternator-client-python/issues/37) | Gzip request compression supports threshold and compression-level configuration. |
| Response compression | Supported | [#65](https://github.com/scylladb/alternator-client-python/issues/65) | Gzip and deflate response decoding is disabled by default and enabled with `with_response_compression(...)`. |
| Header optimization | Supported | [#37](https://github.com/scylladb/alternator-client-python/issues/37) | Required headers are preserved, with static and callback-computed whitelist additions. |
| TLS server trust | Supported | Existing API | System CA, custom CA, hostname verification, and trust-all mode exist. |
| TLS client certificates | Supported | [#38](https://github.com/scylladb/alternator-client-python/issues/38) | mTLS certificate/key paths are loaded into discovery SSL contexts and SDK `client_cert` config. |
| TLS key log file | Supported | [#38](https://github.com/scylladb/alternator-client-python/issues/38) | Debug-only key log file paths are applied to SSL contexts when supported by the runtime. |
| Transport and SDK config knobs | Supported | [#34](https://github.com/scylladb/alternator-client-python/issues/34) | Retry, pool, connect/read timeout, region, and SDK config customizer settings are wired into sync and async SDK clients. |
| Key route affinity | Supported | [#23](https://github.com/scylladb/alternator-client-python/issues/23) | RMW detection, single-write affinity, and BatchWriteItem preferred-node voting are implemented with fallback on missing or ambiguous routing data. |
| Helper lifecycle facade | Supported | [#33](https://github.com/scylladb/alternator-client-python/issues/33) | `Helper` and `AsyncHelper` expose lifecycle, node inspection, topology checks, and partition-key diagnostics. |
| Compatibility and release decisions | Supported | [#39](https://github.com/scylladb/alternator-client-python/issues/39) | Decision record lives in [docs/COMPATIBILITY_AND_RELEASE.md](COMPATIBILITY_AND_RELEASE.md). |
| Node health tracking | Deferred | [#32](https://github.com/scylladb/alternator-client-python/issues/32) | Planning-only. No node health code, tests, config objects, or behavior changes are authorized by this roadmap. |
| Vector search extension | Supported | Existing API | Python client enables ScyllaDB Alternator vector extensions. |
| Capability test harness | Partial | [#36](https://github.com/scylladb/alternator-client-python/issues/36) | Fake Alternator server fixture introduced for deterministic unit tests. |
| Documentation and examples | Supported | [#40](https://github.com/scylladb/alternator-client-python/issues/40) | README, examples, and release-note guidance are aligned with implemented APIs. |

## Deterministic Test Harness

Unit tests can use the `fake_alternator_server` fixture to simulate local
Alternator HTTP behavior without a running Scylla cluster. The fixture supports:

- `/localnodes` JSON responses
- arbitrary HTTP status responses
- request path capture
- deterministic node add/remove scenarios
- transport failure tests by pointing clients at unused ports

Use integration tests for real Scylla behavior and the fake server for edge
cases that need precise control over responses.
