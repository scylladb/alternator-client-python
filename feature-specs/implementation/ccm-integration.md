# CCM integration implementation

This document connects the [CCM integration specification](../ccm-integration.md) to the test
infrastructure in this repository. It is informative: deviations recorded here do not weaken the
generic contract.

## Public API

There is no Python API for CCM cluster specifications, reusable or private leases, resource scopes,
or cluster lifecycle control. Integration tests receive one externally selected endpoint through
the environment variables read by
[`tests/integration/__init__.py`](../../tests/integration/__init__.py).

The repository exposes command-line test targets in the [`Makefile`](../../Makefile), including
`scylla-start`, `scylla-stop`, and `test-integration`. These targets are build conveniences, not an
implementation of the lease API defined by the generic contract.

## Internal architecture

The `scylla-start` target launches [`docker-compose.yml`](../../docker-compose.yml). That manifest
defines a fixed three-node Docker network with fixed container names, addresses, ports, processing
units, and memory. [`tests/scylla/scylla.yaml`](../../tests/scylla/scylla.yaml) supplies one shared
Alternator configuration, and the Makefile generates or restores one shared TLS certificate pair.

This infrastructure does not invoke CCM. It has no typed cluster specification, reuse key, cluster
pool, lease reference count, per-lease resource namespace, loopback-address reservation, private
cluster controller, run manifest, or dirty-cluster state.

## Lifecycle and concurrency

`test-integration` starts the fixed Docker Compose project and installs a shell trap that runs
`docker compose down -v` at normal exit. Explicit Makefile targets can stop, kill, or remove the
containers. Docker Compose owns container and volume cleanup; the Python test process owns no
cluster record.

Fixed container names, host ports, subnet, certificate paths, and Compose project state make this
harness a single shared fixture rather than a coordinated single-slot lease. Parallel test
processes are not assigned independent resource scopes or address reservations. There is no
startup recovery using owner identity, bounded mutation rollback, dirty-state quarantine,
diagnostic snapshot transfer, or retained cleanup state after abrupt termination.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `CCM-REQ-001` | [`docker-compose.yml`](../../docker-compose.yml) is a static manifest, not a typed CCM specification. | — | `gap` |
| `CCM-REQ-002` | [`Makefile`](../../Makefile) provisions through Docker Compose rather than CCM. | [`TestLiveNodeHelperDiagnostics::test_helper_exposes_nodes_clients_and_partition_keys`](../../tests/integration/test_live_nodes.py) | `gap` |
| `CCM-REQ-003` | [`tests/integration/__init__.py`](../../tests/integration/__init__.py) exposes shared endpoint settings but no reusable lease or resource scope. | — | `gap` |
| `CCM-REQ-004` | [`Makefile`](../../Makefile) exposes whole-Compose start, stop, kill, and remove targets but no exclusive private lease or serialized node mutation. | — | `gap` |
| `CCM-REQ-005` | [`docker-compose.yml`](../../docker-compose.yml) fixes one three-node deployment but implements no admission or reuse policy. | — | `gap` |
| `CCM-REQ-006` | [`Makefile`](../../Makefile) performs normal `down -v` cleanup only. | — | `gap` |

## Test coverage

- [`test_live_nodes.py`](../../tests/integration/test_live_nodes.py) proves that synchronous and
  asynchronous helpers can discover and use the already-running cluster.
- [`test_tls.py`](../../tests/integration/test_tls.py) exercises HTTPS against the shared
  certificate fixture.
- [`test_connection_reuse.py`](../../tests/integration/test_connection_reuse.py) exercises client
  connection reuse and concurrent requests, not cluster-lease reuse.
- No test exercises CCM, typed provisioning, independent resource scopes, private topology
  mutation, admission conflicts, address reservation, stale-run recovery, or diagnostic retention.

## Known conformance gaps

- `CCM-REQ-001`: No immutable typed cluster specification or deterministic complete reuse key
  exists. Environment and Make variables select only pieces of the fixed Docker fixture.
- `CCM-REQ-002`: Provisioning uses Docker Compose, not native CCM. Topology, transport, security,
  resources, certificate generation, and readiness cannot be selected through the required typed
  contract.
- `CCM-REQ-003`: There are no reusable leases, read-only cluster views, generated resource
  prefixes, reference counts, or harness-owned table cleanup boundaries. Individual tests perform
  any naming and cleanup themselves.
- `CCM-REQ-004`: There is no exclusive private lease, per-node lifecycle or topology control,
  serialized mutation, bounded rollback, or dirty-cluster handling.
- `CCM-REQ-005`: The static three-node Compose project does not validate the nine-node ceiling,
  compare reuse identities, replace an idle incompatible cluster, or fail incompatible requests
  according to active-lease state. Concurrent harnesses can instead collide on fixed resources.
- `CCM-REQ-006`: Normal shell-trap cleanup has no durable owner/run manifest, command logs,
  diagnostics snapshot, PID/start-time/boot-identity recovery, marked-process reaping, malformed
  state quarantine, validated cleanup root, or address-ID retention after partial failure.
