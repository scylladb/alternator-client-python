# Query-plan implementation

This document connects the [query-plan specification](../query-plan.md) to the implementation in
this repository. It is informative: deviations recorded here do not weaken the generic contract.

## Public API

Query planning is installed automatically for the boto3 and aiobotocore clients created by
[`client.py`](../../alternator/client.py) and
[`async_client.py`](../../alternator/async_client.py). Ordinary operations use randomized plans;
[`config.py`](../../alternator/config.py) exposes `KeyRouteAffinityConfig` and
`KeyRouteAffinityMode` for selecting seeded or preferred affinity plans. `RetryConfig` controls how
many physical attempts the SDK may request. There is no separate public plan or traversal API.

## Internal architecture

[`live_nodes.py`](../../alternator/core/live_nodes.py) publishes immutable `NodeList` snapshots. It
deduplicates and sorts raw host strings at topology ingestion. One configured scheme and port are
then applied to every selected host.

[`handlers.py`](../../alternator/core/handlers.py) registers `update_endpoint` on Botocore's
`request-created.dynamodb.*` event. On first invocation it snapshots `manager.nodes`, computes any
affinity target, creates a plan, and stores the iterator in the request context. Fresh prepared
requests created for SDK retries retain that context and advance the same iterator. Endpoint
replacement changes the URL scheme and authority and rebuilds the URL from `parsed.path` and
`parsed.query`, omitting `parsed.params`. Botocore subsequently signs and transmits the updated
request.

[`query_plan.py`](../../alternator/core/query_plan.py) implements pick-and-remove traversal.
[`deterministic_rand.py`](../../alternator/core/deterministic_rand.py) implements the portable
lagged-Fibonacci generator. Ordinary cycles use a fresh random 64-bit seed. Seeded affinity cycles
reuse the same signed seed, while batch affinity uses its voted-node prefix followed by sorted
remaining hosts. The implementation has no health wrapper or in-flight outcome tracker.

## Lifecycle and concurrency

The candidate snapshot and mutable iterator belong to one logical SDK request. They are created on
the first routing event, stored on the request context, and discarded with that context. Plans are
not shared between logical requests and need no internal lock. `LiveNodesManagerCore` synchronizes
snapshot publication, so a topology update before plan creation is visible and one after creation
does not rewrite the current plan.

Sync and async clients register the same routing handler. Each traffic plan is an unbounded cycle:
random routing creates a new seeded permutation for each cycle, whereas single-item and batch
affinity repeat their original deterministic order. Client shutdown does not own separate query-plan
resources.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `QUERY-REQ-001` | [`handlers.py`](../../alternator/core/handlers.py), [`live_nodes.py`](../../alternator/core/live_nodes.py) | [`test_request_lifecycle.py`](../../tests/unit/test_request_lifecycle.py), [`test_live_nodes.py`](../../tests/unit/test_live_nodes.py) | `gap` |
| `QUERY-REQ-002` | [`handlers.py`](../../alternator/core/handlers.py), [`query_plan.py`](../../alternator/core/query_plan.py) | [`test_query_plan.py`](../../tests/unit/test_query_plan.py) | `gap` |
| `QUERY-REQ-003` | [`handlers.py`](../../alternator/core/handlers.py), [`live_nodes.py`](../../alternator/core/live_nodes.py), [`query_plan.py`](../../alternator/core/query_plan.py), [`deterministic_rand.py`](../../alternator/core/deterministic_rand.py) | [`test_query_plan.py`](../../tests/unit/test_query_plan.py), [`test_deterministic_rand.py`](../../tests/unit/test_deterministic_rand.py) | `gap` |
| `QUERY-REQ-004` | [`handlers.py`](../../alternator/core/handlers.py) | [`test_key_affinity.py`](../../tests/unit/test_key_affinity.py) | `gap` |
| `QUERY-REQ-005` | [`handlers.py`](../../alternator/core/handlers.py) | [`test_request_lifecycle.py`](../../tests/unit/test_request_lifecycle.py) | `gap` |
| `QUERY-REQ-006` | [`handlers.py`](../../alternator/core/handlers.py), [`live_nodes.py`](../../alternator/core/live_nodes.py) | [`test_key_affinity.py`](../../tests/unit/test_key_affinity.py), [`test_request_lifecycle.py`](../../tests/unit/test_request_lifecycle.py) | `gap` |
| `QUERY-REQ-007` | [`handlers.py`](../../alternator/core/handlers.py), [`request.py`](../../alternator/core/request.py) | [`test_request_lifecycle.py`](../../tests/unit/test_request_lifecycle.py), [`test_async_client.py`](../../tests/integration/test_async_client.py) | `gap` |

## Test coverage

- [`test_query_plan.py`](../../tests/unit/test_query_plan.py) covers exhaustion, uniqueness,
  distribution, seed stability, edge-case seeds, and portable seeded-order prefixes.
- [`test_deterministic_rand.py`](../../tests/unit/test_deterministic_rand.py) covers generator state,
  reference outputs, bounded selection, and zero, negative, and maximum seeds.
- [`test_request_lifecycle.py`](../../tests/unit/test_request_lifecycle.py) exercises real Botocore
  request creation, URL mutation before signing, retry traversal across fresh prepared requests,
  deterministic cycle repetition, and connection reuse after server responses.
- [`test_key_affinity.py`](../../tests/unit/test_key_affinity.py) covers seeded and multi-preferred
  plans, sorted remainder order, repeated cycles, and IPv6 authority formatting.
- [`test_live_nodes.py`](../../tests/unit/test_live_nodes.py) covers immutable sorted node snapshots,
  raw-host deduplication, concurrent access, and topology refresh behavior.
- [`test_async_client.py`](../../tests/integration/test_async_client.py) exercises the shared handler
  stack through the asynchronous SDK client.

## Known conformance gaps

- `QUERY-REQ-001`: Request ownership and lazy snapshot timing conform, but endpoint normalization
  does not. Routing state contains host-only strings and applies one shared scheme and port. It does
  not parse arbitrary endpoint URIs, remove user information/path/query/fragment components,
  lowercase scheme and host, collapse explicit default ports, or retain the first representative
  spelling required by `endpoint-order.tsv`.
- `QUERY-REQ-002`: Random traversal permutes raw node-address strings. Deduplication uses raw string
  equality, so canonically equivalent hosts such as `A` and `a` can both be returned in one cycle.
- `QUERY-REQ-003`: The generator and pick-and-remove behavior match the portable vectors when inputs
  are already canonical. Production preprocessing sorts and deduplicates raw host strings, so equal
  canonical candidate sets with different spellings can produce different orders or duplicates.
- `QUERY-REQ-004`: Preferred membership, deduplication, and suffix sorting compare raw host strings.
  A canonically equivalent spelling can fail to match or appear twice, and mixed-case suffixes can
  differ from canonical endpoint order.
- `QUERY-REQ-005`: URL replacement, parsed-path/query preservation, retry reuse, body stability, and
  pre-signing order are implemented. Node-health attempt attribution is absent: the client does not
  record an in-flight endpoint or classify a final response versus a pre-final-response transport
  failure against that endpoint. If a later attempt loses its request context, the handler
  constructs a new plan instead of failing locally.
- `QUERY-REQ-006`: Active-node cycle behavior is implemented, including random reshuffling and
  stable affinity repetition. Node health and probe plans are not implemented, so plans cannot
  perform active-before-quarantine passes, exclude down nodes, recheck eligibility before each
  selection, or produce the specified no-route result when only down nodes remain.
- `QUERY-REQ-007`: Sync and async clients share the same routing handler, which preserves the method,
  headers, body, `parsed.path`, and `parsed.query`. However, `urlparse` puts a final-segment URI
  parameter such as `tenant=x` in `parsed.params` for `/v1;tenant=x`, while `update_endpoint` omits
  that field when rebuilding the URL. The routed URL therefore loses that path data.
