# Node-health implementation

This document connects the [node-health specification](../node-health.md) to the implementation in
this repository. It is informative: deviations recorded here do not weaken the generic contract.

## Public API

No node-health configuration, state, observation, or status type is exposed by
[`Config`](../../alternator/config.py). The public compatibility facades
[`Helper`](../../alternator/client.py) and [`AsyncHelper`](../../alternator/async_client.py) expose
`get_active_nodes()` and `get_quarantined_nodes()`, but the former aliases the complete discovered
node list and the latter always returns an empty list.

[`NoNodesAvailableError`](../../alternator/exceptions.py) reports local routing exhaustion. It does
not expose health state or result from health-aware filtering.

## Internal architecture

[`LiveNodesManagerCore`](../../alternator/core/live_nodes.py) stores one immutable, sorted,
deduplicated `NodeList` and selects from it without health metadata. Successful `/localnodes`
refreshes replace that list; failed or empty refreshes retain the prior list. Every listed endpoint
is immediately eligible for traffic.

[`_register_alternator_handlers`](../../alternator/core/handlers.py) creates request-scoped plans
from the entire discovered list. [`LazyQueryPlan`](../../alternator/core/query_plan.py) provides
seeded pick-and-remove order, while the handler recreates plans to form retry cycles. There is no
final health gate, attempt outcome observer, health store, generation token, or health-neutral
response classification.

`SyncLiveNodesManager` and `AsyncLiveNodesManager` in
[`live_nodes.py`](../../alternator/core/live_nodes.py) schedule topology refreshes only. They do not
schedule direct validation or recovery probes.

## Lifecycle and concurrency

The synchronous manager serializes refreshes, protects the node snapshot, and stops its background
thread with a bounded join. The asynchronous manager serializes refreshes with a lock and cancels
its background task on close. These lifecycle rules apply only to topology discovery.

Requests retain one iterator across SDK retries, but no attempt captures a health generation and no
completion reports an endpoint observation. There are no probe queues, priorities, per-endpoint
deduplication, timeouts, suppression, shared probe results, or shutdown handling for health work.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `HEALTH-REQ-001` | [`Config`](../../alternator/config.py) has topology polling and transport timeouts but no node-health settings. | [`TestAlternatorConfig::test_default_values`](../../tests/unit/test_config.py) | `gap` |
| `HEALTH-REQ-002` | [`_register_alternator_handlers`](../../alternator/core/handlers.py) routes attempts but does not classify or report outcomes. | — | `gap` |
| `HEALTH-REQ-003` | [`LiveNodesManagerCore`](../../alternator/core/live_nodes.py) has no health state machine or counters. | — | `gap` |
| `HEALTH-REQ-004` | [`LiveNodesManagerCore`](../../alternator/core/live_nodes.py) has no attempt generations or stale-result rejection. | — | `gap` |
| `HEALTH-REQ-005` | [`LiveNodesManagerCore`](../../alternator/core/live_nodes.py) publishes discovered endpoints directly as routable nodes. | [`test_helper_lifecycle_and_node_diagnostics`](../../tests/unit/test_helper.py) | `gap` |
| `HEALTH-REQ-006` | [`_register_alternator_handlers`](../../alternator/core/handlers.py) traverses all snapshotted nodes without active/quarantine/down filtering. | [`test_sdk_retries_advance_shared_query_plan`](../../tests/unit/test_request_lifecycle.py) | `gap` |
| `HEALTH-REQ-007` | [`SyncLiveNodesManager` and `AsyncLiveNodesManager`](../../alternator/core/live_nodes.py) schedule topology refreshes, not health probes. | [`test_async_stop_cancels_in_flight_discovery`](../../tests/unit/test_refresh_lifecycle.py) | `gap` |
| `HEALTH-REQ-008` | [`SyncLiveNodesManager._refresh_nodes` and `AsyncLiveNodesManager._refresh_nodes`](../../alternator/core/live_nodes.py) try configured seeds and scopes without health tiers. | [`TestSyncLiveNodesManager::test_refresh_failure_keeps_existing_nodes`](../../tests/unit/test_sync_manager.py) | `gap` |
| `HEALTH-REQ-009` | [`Helper`](../../alternator/client.py) and [`AsyncHelper`](../../alternator/async_client.py) have lifecycle parity only for discovery and expose placeholder health views. | [`test_async_helper_lifecycle_and_node_diagnostics`](../../tests/unit/test_helper.py) | `gap` |

## Test coverage

- [`test_live_nodes.py`](../../tests/unit/test_live_nodes.py) covers immutable discovered-node
  snapshots, sorted deduplication, round-robin selection, and thread safety without health states.
- [`test_sync_manager.py`](../../tests/unit/test_sync_manager.py) and
  [`test_async_manager.py`](../../tests/unit/test_async_manager.py) cover topology refresh, scope
  fallback, and retention of an old list after refresh failure.
- [`test_refresh_lifecycle.py`](../../tests/unit/test_refresh_lifecycle.py) covers discovery refresh
  serialization and shutdown, not health-probe coordination.
- [`test_request_lifecycle.py`](../../tests/unit/test_request_lifecycle.py) proves that SDK retries
  traverse and repeat a request plan, without outcome classification or health eligibility.
- [`test_helper.py`](../../tests/unit/test_helper.py) explicitly verifies current compatibility
  behavior: every discovered node appears active and quarantine is empty for both client forms.
- [`test_node_health.py`](../../tests/integration/test_node_health.py) exercises successful
  discovery, periodic refresh, SDK retries, and concurrent requests. Its tests explicitly note that
  quarantine is not implemented and do not create health-state transitions or direct probes.
- The portable [`node-health-transitions.tsv`](../vectors/node-health-transitions.tsv) cases are not
  executed by any Python test.

## Known conformance gaps

- `HEALTH-REQ-001`: All health thresholds, probe period, probe concurrency, probe timeout,
  normalization, validation, and explicit disabled configuration are absent. Current polling and
  transport timeout settings are different features.
- `HEALTH-REQ-002`: DynamoDB attempts and control-plane calls produce no health observations.
  Retryable server statuses, application responses, and pre-final-response failures therefore
  cannot update or preserve health as specified.
- `HEALTH-REQ-003`: There are no `ACTIVE`, `QUARANTINED`, or `DOWN` records, independent traffic and
  probe counters, transition thresholds, or executable transition-vector coverage.
- `HEALTH-REQ-004`: Routed attempts capture no endpoint generation. Late results cannot be rejected
  relative to a later down-and-recovery cycle because no such cycle is represented.
- `HEALTH-REQ-005`: Seeds and indirectly listed endpoints are placed directly in the common
  routable list. Direct contact is not required, removed nodes retain no health history, and the
  active/quarantine helper methods are compatibility placeholders.
- `HEALTH-REQ-006`: Query plans do not prioritize active endpoints, defer quarantine, exclude down
  endpoints, or recheck eligibility before each selection. They cycle through every snapshotted
  endpoint until the SDK retry policy stops.
- `HEALTH-REQ-007`: Periodic `/localnodes` refresh is not a direct health-probe subsystem. Explicit
  probes, recovery probing, bounded concurrency, priority, per-endpoint sharing, suppression,
  timeout abortion, and health-probe shutdown are absent.
- `HEALTH-REQ-008`: Discovery walks configured scopes and seeds without active/quarantine/down
  partitions. Indirect membership is trusted for traffic immediately, and down-node fallback and
  recovery-counter isolation do not exist.
- `HEALTH-REQ-009`: Sync and async discovery managers have bounded cleanup, but no node-health
  lifecycle or decisions to compare. Node health also cannot be explicitly disabled, despite the
  generic default requiring it to be enabled.
