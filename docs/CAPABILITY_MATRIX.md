# Alternator Client Capability Matrix

This matrix tracks current Python Alternator client capabilities, planned work,
and intentionally deferred behavior.

| Capability | Status | Tracking | Notes |
| --- | --- | --- | --- |
| Sync DynamoDB client | Supported | Existing API | `create_client`, `AlternatorClient`, and `alternator.client(...)` return standard boto3 clients. |
| DynamoDB resource | Supported | Existing API | `create_resource` and `AlternatorResource` wrap boto3 resource usage. |
| Async DynamoDB client | Supported | Existing API | `create_async_client` and `AsyncAlternatorClient` use aioboto3. |
| Host-only seeds with one shared port | Supported | Existing API | Seeds must not include ports; one port applies to all nodes. |
| Node discovery | Supported | Existing API | `/localnodes` refresh updates the live node list. |
| Routing scopes | Partial | [#35](https://github.com/scylladb/alternator-client-python/issues/35) | Cluster, datacenter, and rack scopes exist; explicit fallback control and validation are planned. |
| Request query plans | Supported | Existing API | Requests use stable seeded node ordering for retries. |
| Auth | Supported | Existing API | Disabled by default; explicit static credentials enable signing. |
| Request compression | Partial | [#37](https://github.com/scylladb/alternator-client-python/issues/37) | Gzip exists; configurable levels and custom compressors are planned. |
| Header optimization | Partial | [#37](https://github.com/scylladb/alternator-client-python/issues/37) | Basic whitelist exists; custom whitelist callback is planned. |
| TLS server trust | Supported | Existing API | System CA, custom CA, hostname verification, and trust-all mode exist. |
| TLS client certificates | Missing | [#38](https://github.com/scylladb/alternator-client-python/issues/38) | mTLS config and SDK propagation are planned. |
| TLS key log file | Missing | [#38](https://github.com/scylladb/alternator-client-python/issues/38) | Debug-only key log file support is planned. |
| Transport and SDK config knobs | Partial | [#34](https://github.com/scylladb/alternator-client-python/issues/34) | Retry and pool config exist; connect/read timeout and customizer wiring are planned. |
| Key route affinity | Partial | [#23](https://github.com/scylladb/alternator-client-python/issues/23) | Partition-key cache exists; RMW rules and BatchWriteItem voting need alignment with the request-routing specification. |
| Helper lifecycle facade | Supported | [#33](https://github.com/scylladb/alternator-client-python/issues/33) | `Helper` and `AsyncHelper` expose lifecycle, node inspection, topology checks, and partition-key diagnostics. |
| Node health tracking | Deferred | [#32](https://github.com/scylladb/alternator-client-python/issues/32) | Planning-only. No node health code, tests, config objects, or behavior changes are authorized by this roadmap. |
| Vector search extension | Supported | Existing API | Python client enables ScyllaDB Alternator vector extensions. |
| Capability test harness | Partial | [#36](https://github.com/scylladb/alternator-client-python/issues/36) | Fake Alternator server fixture introduced for deterministic unit tests. |
| Documentation and examples | Partial | [#40](https://github.com/scylladb/alternator-client-python/issues/40) | README and examples should stay aligned with implemented APIs. |

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
