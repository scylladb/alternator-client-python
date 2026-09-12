# Key-route affinity implementation

This document connects the [key-route-affinity specification](../key-route-affinity.md) to the
implementation in this repository. It is informative: deviations recorded here do not weaken the
generic contract.

## Public API

[`config.py`](../../alternator/config.py) exposes `KeyRouteAffinityMode` values `NONE`, `RMW`, and
`ANY_WRITE`, `KeyRouteAffinityConfig`, and
`AlternatorConfigBuilder.with_key_affinity(...)`. `Config.key_affinity` accepts the mode and an
optional table-to-partition-key mapping. These names are re-exported from
[`alternator/__init__.py`](../../alternator/__init__.py).

Affinity is installed automatically for clients and resources created through
[`client.py`](../../alternator/client.py) and for clients created through
[`async_client.py`](../../alternator/async_client.py). `Helper.get_partition_key_name(...)` and
`AsyncHelper.get_partition_key_name(...)` provide explicit metadata diagnostics. There is no public
hash or affinity-plan lifecycle API.

## Internal architecture

[`key_affinity.py`](../../alternator/core/key_affinity.py) classifies operations, extracts scalar
partition keys, builds seeded single-item targets, and aggregates `BatchWriteItem` votes. Batch
metadata is snapshotted once per table for the request; votes are sorted by descending count and
then node address. [`hashing.py`](../../alternator/core/hashing.py) implements typed `S`, `N`, and
`B` encoding and MurmurHash3 x64 128-bit hashing.

Synchronous metadata discovery is owned by `PartitionKeyCache` in
[`key_affinity.py`](../../alternator/core/key_affinity.py). It uses one bounded daemon worker,
per-table pending events, and a locked positive-result cache. `AsyncPartitionKeyCache` in
[`async_client.py`](../../alternator/async_client.py) uses per-table tasks, events, and an
`asyncio.Lock` to provide the corresponding asynchronous behavior. Both request paths call the
non-blocking cached lookup: a miss starts `DescribeTable`, while that request uses random routing.

[`handlers.py`](../../alternator/core/handlers.py) parses affinity inputs at Botocore's
`request-created` event. A `SeededAffinityPlan` creates a deterministic
[`LazyQueryPlan`](../../alternator/core/query_plan.py); a batch preference tuple creates a fixed
preferred prefix followed by the remaining sorted nodes. The chosen plan is stored on the request
context and reused by SDK retries.

## Lifecycle and concurrency

Configuration copies the caller's table mapping. Metadata misses are coalesced per table, and
successful results are cached until explicit clearing or client close. The synchronous worker has a
queue capacity of 64 and a one-second default join bound. `close_client(...)`, helper shutdown, and
context-manager shutdown close the cache before stopping discovery and the SDK transport. An active
synchronous `DescribeTable` call cannot be cancelled and may outlive that bounded join.

The asynchronous cache cancels and awaits client-owned discovery tasks during
`close_async_client(...)`. Sync and async request routing share the same classifier, hasher, batch
voter, and query-plan handler. Each request owns its plan; shared live-node snapshots and metadata
caches provide their own synchronization.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `AFF-REQ-001` | [`config.py`](../../alternator/config.py), [`key_affinity.py`](../../alternator/core/key_affinity.py) | [`test_key_affinity.py`](../../tests/unit/test_key_affinity.py) | `conformant` |
| `AFF-REQ-002` | [`key_affinity.py`](../../alternator/core/key_affinity.py), [`async_client.py`](../../alternator/async_client.py) | [`test_key_affinity.py`](../../tests/unit/test_key_affinity.py), [`test_async_manager.py`](../../tests/unit/test_async_manager.py), [`test_helper.py`](../../tests/unit/test_helper.py) | `gap` |
| `AFF-REQ-003` | [`hashing.py`](../../alternator/core/hashing.py), [`key_affinity.py`](../../alternator/core/key_affinity.py) | [`test_hashing.py`](../../tests/unit/test_hashing.py), [`test_hashing_properties.py`](../../tests/unit/test_hashing_properties.py) | `gap` |
| `AFF-REQ-004` | [`key_affinity.py`](../../alternator/core/key_affinity.py), [`handlers.py`](../../alternator/core/handlers.py) | [`test_key_affinity.py`](../../tests/unit/test_key_affinity.py), [`test_request_lifecycle.py`](../../tests/unit/test_request_lifecycle.py) | `conformant` |
| `AFF-REQ-005` | [`key_affinity.py`](../../alternator/core/key_affinity.py), [`handlers.py`](../../alternator/core/handlers.py) | [`test_key_affinity.py`](../../tests/unit/test_key_affinity.py), [`test_key_affinity_operations.py`](../../tests/integration/test_key_affinity_operations.py) | `conformant` |
| `AFF-REQ-006` | [`handlers.py`](../../alternator/core/handlers.py), [`live_nodes.py`](../../alternator/core/live_nodes.py) | [`test_key_affinity.py`](../../tests/unit/test_key_affinity.py) | `gap` |
| `AFF-REQ-007` | [`key_affinity.py`](../../alternator/core/key_affinity.py), [`client.py`](../../alternator/client.py), [`async_client.py`](../../alternator/async_client.py) | [`test_key_affinity.py`](../../tests/unit/test_key_affinity.py), [`test_async_manager.py`](../../tests/unit/test_async_manager.py) | `gap` |

## Test coverage

- [`test_key_affinity.py`](../../tests/unit/test_key_affinity.py) covers the complete RMW operation
  matrix, affinity modes, key extraction, malformed union fallback, single-item planning, batch
  voting and ties, metadata snapshotting, cache concurrency, queue bounds, and shutdown.
- [`test_hashing.py`](../../tests/unit/test_hashing.py) executes the portable typed hash values;
  [`test_hashing_properties.py`](../../tests/unit/test_hashing_properties.py) adds property-based
  determinism, type separation, signed-range, and plan-selection coverage.
- [`test_query_plan.py`](../../tests/unit/test_query_plan.py) covers the seeded permutations used by
  affinity, including zero, negative, and maximum signed 64-bit seeds.
- [`test_request_lifecycle.py`](../../tests/unit/test_request_lifecycle.py) verifies that SDK retries
  retain and advance a single seeded affinity plan and that routing and compression finish before
  signing.
- [`test_async_manager.py`](../../tests/unit/test_async_manager.py) covers asynchronous metadata
  caching, miss coalescing, cancellation, retry after cancellation, and client cleanup.
- [`test_key_affinity_operations.py`](../../tests/integration/test_key_affinity_operations.py) and
  [`test_composite_keys.py`](../../tests/integration/test_composite_keys.py) exercise real DynamoDB
  operations, `BatchWriteItem`, metadata discovery, and HASH-key-only routing.

## Known conformance gaps

- `AFF-REQ-002`: Preconfiguration, positive caching, and one in-flight lookup per table exist, but
  discovery has no feature-owned one-plus-three attempt policy, 100-millisecond exponential backoff,
  jitter, permanent/transient error classification, five-minute cooldown, or explicit
  permanent-failure record clearing. SDK retries may occur underneath `DescribeTable`, but they do
  not implement this contract. Failed or HASH-less results are not negatively cached, so a later
  request may schedule another lookup immediately.
- `AFF-REQ-003`: Supported well-formed values match every portable hash vector, and malformed union
  shapes fall back. Scalar validation is incomplete: number strings accepted by `float`, including
  non-finite, whitespace-padded, and out-of-range values, can still produce affinity hashes, while
  some non-mapping key shapes raise during extraction instead of selecting random fallback.
- `AFF-REQ-006`: Node-health tracking is not implemented. Affinity hashes and votes use the stable
  discovered node snapshot, but every discovered node is treated as active; there are no retained
  down/quarantined identities or final active-before-quarantine health gate.
- `AFF-REQ-007`: Sync and async forms produce equivalent hashes and orders when given equivalent
  metadata, and async shutdown cancels and awaits its discovery tasks. A synchronous discovery call
  already executing inside Botocore cannot be cancelled, however, so the daemon worker may retain
  the cache and SDK client after the bounded close wait returns.
