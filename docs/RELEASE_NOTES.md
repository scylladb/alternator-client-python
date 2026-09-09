# Release Notes

## 2.0.0 (2026-09-09)

Version 2.0.0 is a major release because it changes the default topology
fallback behavior and the concrete type returned by
`AlternatorConfigBuilder.build()`. It also adds vector search, higher-level
lifecycle helpers, explicit configuration and authentication APIs, compression
controls, IPv6 and DNS recovery, and substantial routing and lifecycle
correctness fixes.

### Breaking Changes

#### Datacenter and Rack Scopes No Longer Fall Back Implicitly

In 1.0.0, `DatacenterScope("dc1")` automatically fell back to cluster scope
when no node was found in `dc1`. `RackScope("dc1", "rack1")` automatically
fell back first to its datacenter and then to the cluster.

In 2.0.0, both constructors remain constrained to the requested scope by
default. A missing or unavailable node in that scope therefore causes the
operation to fail instead of silently routing it more broadly. Callers that
require the 1.0.0 behavior must configure the fallback chain explicitly:

```python
from alternator import ClusterScope, Config, DatacenterScope, RackScope

dc_config = Config(
    seed_hosts=["node1"],
    port=8000,
    routing_scope=DatacenterScope("dc1", fallback=ClusterScope()),
)

rack_config = Config(
    seed_hosts=["node1"],
    port=8000,
    routing_scope=RackScope(
        "dc1",
        "rack1",
        fallback=DatacenterScope("dc1", fallback=ClusterScope()),
    ),
)
```

Review every use of `DatacenterScope`, `RackScope`, builder
`.with_datacenter(...)`, and builder `.with_rack(...)` before upgrading. The
builder methods now select constrained scopes; construct `Config` directly when
an explicit fallback chain is needed.

#### The Configuration Builder Returns `Config`

`AlternatorConfigBuilder.build()` returned an `AlternatorConfig` instance in
1.0.0. It returns `Config` in 2.0.0. Normal attribute access is unchanged, but
code that annotates the result as `AlternatorConfig`, checks
`isinstance(config, AlternatorConfig)`, or dispatches on that concrete type must
be updated to use `Config`.

`AlternatorConfig` and `TlsConfig` remain as deprecated compatibility names and
emit deprecation warnings when instantiated. New code should use `Config` and
`TLS`.

#### Async Dependency Floor Updated

The `async` extra now requires `aiohttp>=3.14.3`. Upgrade environments that pin
an older `aiohttp` before installing 2.0.0. This floor includes fixes for known
security vulnerabilities in older releases.

### Added

- Added ScyllaDB Alternator vector search support to all sync and async clients
  and resources, including vector index request shapes, the `FLOAT32VECTOR`
  wire type, and the high-level `alternator.vector.Vector` value type. Vector
  search currently requires a ScyllaDB Cloud cluster with the feature enabled;
  it is not provided by the stock local ScyllaDB image or AWS DynamoDB.
- Added the top-level `alternator.client(...)` context manager and the `Helper`
  and `AsyncHelper` lifecycle facades. Helpers own clients/resources and expose
  node refresh, node inspection, topology validation, and partition-key cache
  diagnostics.
- Added preferred `Config` and `TLS` APIs. TLS configuration now supports custom
  trust roots, client certificates and keys, hostname verification, and TLS key
  logs for protected debugging environments.
- Added the explicit `Auth` API. Requests remain unsigned by default;
  `Auth.static_credentials(...)` enables signing with static Alternator
  credentials. AWS environment, profile, and provider-chain credentials are not
  used implicitly. Legacy raw credential keyword arguments remain available but
  are deprecated.
- Added typed retry, connection-pool, discovery/connect/read timeout, and AWS
  region-placeholder settings.
- Added control over the final wire `User-Agent`. The default is
  `alternator-client-python/<version>`; a string replaces it, a callback can
  extend it, and `None` omits it.
- Expanded request compression with a configurable size threshold and gzip
  level, and added callback-computed header whitelist entries. Request
  compression remains opt-in and requires ScyllaDB 2026.1.0 or later.
- Added opt-in gzip and deflate response decompression. Server support is
  required for compressed responses.

### Routing, Discovery, and Lifecycle Fixes

- Added IPv6 literal and dual-stack support for discovery and routed SDK
  endpoints, including correctly bracketed IPv6 URLs.
- Improved DNS seed recovery. Discovery tries multiple resolved addresses
  within one bounded deadline, re-resolves DNS on later connections, advances
  through configured seed hosts before an explicit scope fallback, and can
  recover after DNS, connection, timeout, invalid-response, and empty-response
  failures.
- Cluster-scoped discovery now combines `/localnodes` results from every
  configured seed. Because an optionless `/localnodes` response is local to the
  contacted seed's datacenter, multi-datacenter discovery requires at least one
  reachable seed in each datacenter.
- Request-scoped node plans remain stable within an SDK request and advance
  between SDK retry attempts. Affinity-selected plans are preserved across
  those retries.
- Closing clients and resources now stops owned discovery managers, closes the
  underlying boto/aioboto sessions, and cleans up partition-key discovery.
  Resource-derived objects retain their manager for the necessary lifetime, and
  async creation/cleanup is cancellation-safe.
- Active traffic now wakes discovery after an idle interval. Concurrent refresh
  and stop/start paths are serialized to avoid overlapping work and orphaned
  refresh tasks.
- Fixed signed-request mutation ordering, preservation of SigV4 headers during
  header optimization, custom-only TLS trust roots, and retry-attempt mapping to
  the SDK.

### Key-Affinity Changes

- Refined `RMW` classification so affinity applies to writes that require prior
  item state. `BatchWriteItem` does not use affinity in `RMW` mode.
- `ANY_WRITE` uses affinity for single-item writes. For `BatchWriteItem`, valid
  put/delete entries vote for preferred nodes; nodes are ordered by descending
  vote count and then address before the remaining query plan.
- Missing table metadata, incomplete keys, and unsupported key values fall back
  to normal routing. Partition-key metadata discovery runs off the request path;
  use `table_pk_map` when affinity must apply on the first request.
- Fixed deterministic hashing of binary keys, expression and legacy-request
  classification, affinity-plan retry behavior, and batch voting edge cases.

### Upgrade Checklist

1. Replace implicit datacenter/rack fallback assumptions with an explicit
   `fallback=...` chain where broader routing is intended.
2. Change builder-result annotations and concrete-type checks from
   `AlternatorConfig` to `Config`.
3. Prefer `Config` and `TLS` over the deprecated `AlternatorConfig` and
   `TlsConfig` names.
4. Replace raw SDK credential keyword arguments with
   `auth=Auth.static_credentials(access_key_id, secret_access_key)`.
5. Confirm request and response compression support in the target ScyllaDB
   version before enabling either feature.
6. Upgrade `aiohttp` to 3.14.3 or later when using the `async` extra.
7. Confirm that ScyllaDB Cloud vector search is enabled before using vector
   indexes, `VectorSearch`, or `FLOAT32VECTOR`.
8. Preload `table_pk_map` if key affinity must apply to a table's first request,
   and review the updated `RMW` and `BatchWriteItem` behavior.
9. Treat SDK connect/read timeouts as per-attempt settings; apply an
   application-level deadline when a whole-operation limit is required.

### Intentionally Deferred

Node health scoring, quarantine, decommission handling, and dead-node handling
are not part of 2.0.0. `get_quarantined_nodes()` continues to return an empty
list.
