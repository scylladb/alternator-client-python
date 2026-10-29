# Release Notes

## Unreleased

### Fixes

- Let the SDK parse HTTP errors with non-object JSON roots, invalid UTF-8, or
  corrupt supported compression through its empty-body fallback. Sync clients,
  resources, and async clients now retain status-based retries and final-response
  metadata for these errors. HTTP success parsing behavior remains unchanged.
- Decode combined gzip, deflate, and identity response encoding chains in reverse
  application order. Validate the entire chain and finish decoding before changing
  the parser body or compression headers; unsupported chains remain unchanged.
- Bound Python SDK response expansion to 64 MiB (67,108,864 bytes) of output per
  layer by default, including intermediate output and concatenated gzip members.
  Configure a positive integer byte ceiling with
  `ResponseCompressionConfig.max_decompressed_bytes`, alongside accepted response
  encodings, or `with_response_compression(..., max_decompressed_bytes=...)` in
  the builder. Over-limit HTTP errors use the SDK
  empty-body fallback; over-limit successes raise `ValueError` with the original
  parser body and headers intact. HTTP input and accepted output remain buffered;
  this limits decoder output rather than total process memory. The experimental
  native backend keeps its fixed 32 MiB policy and rejects nondefault overrides
  of this Python SDK setting.

## 2.0.1 (2026-10-20)

This patch release fixes authenticated response-compression negotiation and
rack-aware key-affinity membership, and reduces request-routing CPU overhead.
Public configuration and authentication defaults are unchanged.

### Fixes

- Finalize `Accept-Encoding` response-compression negotiation before SigV4
  signing. Configured gzip or deflate negotiation no longer changes a signed
  `identity` header after signing. Explicit custom encoding values remain
  unchanged. ([#127](https://github.com/scylladb/alternator-client-python/pull/127),
  [#120](https://github.com/scylladb/alternator-client-python/issues/120))
- Give affinity-eligible requests a separately refreshed datacenter-wide view
  when built-in rack scopes are configured, so clients in different racks use
  the same eligible coordinator membership. Ordinary requests retain their
  configured routing scope. Preserve the explicit fallback chain and order;
  add no implicit fallback. Sync and async clients retain their established
  startup, discovery-failure, and request-snapshot behavior.
  ([#122](https://github.com/scylladb/alternator-client-python/pull/122))

### Performance

- Reconstruct deterministic PRNG seed slots on demand instead of initializing
  all 607 slots for every request. Preserve deterministic values, rejection
  sampling, reseeding, state wraparound, and complete retry order. This targets
  the usual short query plan; fully exhausting very large plans can cost more
  CPU than eager initialization.
  ([#124](https://github.com/scylladb/alternator-client-python/pull/124))
- Skip random-generator construction for empty and one-node plans, and return
  the final remaining node without another random draw. Emitted node order
  remains unchanged.
  ([#125](https://github.com/scylladb/alternator-client-python/pull/125))

### Release Verification

- Production releases stop when an open issue has the `release-blocker` label,
  with fresh checks before tag publication, PyPI publication, and GitHub Release
  creation. ([#117](https://github.com/scylladb/alternator-client-python/pull/117))
- Verify new release signatures with the public key published by
  `scylladb-publisher`, fingerprint
  `71A6D22711CDB7C2446D21CFBF4BF97A8D4DF1AA`. Existing 2.0.0 signatures retain
  their original verification key.
  ([#126](https://github.com/scylladb/alternator-client-python/pull/126))

## 2.0.0 (2026-09-09)

Version 2.0.0 is a major release because it changes the default topology
fallback behavior and the concrete type returned by
`AlternatorConfigBuilder.build()`. It also adds higher-level lifecycle helpers,
explicit configuration and authentication APIs, compression controls, IPv6 and
DNS recovery, and substantial routing and lifecycle correctness fixes.

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
- SDK invocation/retry and Lambda recursion-detection headers are no longer
  constructed, and SDK User-Agent rebuilding is removed. Whitelist filtering
  remains opt-in; signing, retry state, and the configured Alternator User-Agent
  are preserved.
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
7. Preload `table_pk_map` if key affinity must apply to a table's first request,
   and review the updated `RMW` and `BatchWriteItem` behavior.
8. Treat SDK connect/read timeouts as per-attempt settings; apply an
   application-level deadline when a whole-operation limit is required.

### Intentionally Deferred

Node health scoring, quarantine, decommission handling, and dead-node handling
are not part of 2.0.0. `get_quarantined_nodes()` continues to return an empty
list.
