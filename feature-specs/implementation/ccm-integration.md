# CCM integration implementation

This document connects the [CCM integration specification](../ccm-integration.md) to the test
infrastructure in this repository. It is informative: deviations recorded here do not weaken the
generic contract.

## Public API

[`TestClusters`](../../tests/testinfra/pool.py) exposes synchronous
`acquire_reusable(ClusterSpec)` and `provision_private(ClusterSpec)` entry points. Both return
context-manager leases. [`ReusableClusterLease`](../../tests/testinfra/cluster.py) exposes
read-only cluster information and a resource scope; `PrivateClusterLease` additionally exposes
`PrivateClusterControl`.

[`ClusterSpec`](../../tests/testinfra/cluster_spec.py) provides immutable `with_*` methods for
version, topology, transports, security, per-node resources, and YAML overrides. Override keys are
canonicalized and values use safe YAML 1.2 Core parsing. `ClusterSpecs.default_spec()` applies
`SCYLLA_VERSION`, defaulting to `release:2025.2.5`.

This API is public to repository tests through [`tests.testinfra`](../../tests/testinfra/__init__.py)
but is not packaged as part of the production `alternator-client` library. Operational environment
variables are `SCYLLA_CCM_PATH`, `SCYLLA_CCM_ROOT`, `SCYLLA_CCM_MAX_NODES`,
`SCYLLA_CCM_DIAGNOSTICS_DIR`, and `SCYLLA_VERSION`.

## Internal architecture

[`TestClusterPool`](../../tests/testinfra/pool.py) owns one physical-cluster slot, lease reference
counts, resource scopes, normal shutdown, and durable ownership. Matching reusable acquisitions
share the slot. An idle incompatible cluster is removed and replaced; incompatible acquisitions
fail immediately while leases are active. There is no memory budget, wait queue, or LRU scheduler.

[`CcmProvisioner`](../../tests/testinfra/ccm_provisioner.py) translates topology into CCM
create/add commands, writes typed Scylla configuration, generates a CA and per-node certificates
with OpenSSL, waits for HTTP and HTTPS readiness without proxies, and writes durable per-command
logs. It validates CCM metadata and live PID identity before lifecycle commands. Failed or
ambiguous mutation dirties the cluster and prevents further reuse or mutation.

[`CcmRunState`](../../tests/testinfra/run_state.py) owns private run directories, atomic manifests,
cross-process loopback-range reservations, owner identity, safe stale-process reaping, diagnostic
recovery, and quarantine. [`ccm_install`](../../tests/testinfra/ccm_install.py) validates or repairs
an isolated virtual environment containing the pinned CCM commit under a cross-process lock.

## Lifecycle and concurrency

Each process owns at most one physical cluster. Matching reusable leases may coexist with separate
resource prefixes. Idle reuse includes endpoint health validation. A private lease is exclusive and
provides serialized cluster/node start, stop, add, and remove operations. Releasing a reusable
lease deletes only tables in its namespace; HTTPS-only cleanup uses the generated CA.

The private per-user default state root records owner PID, Linux start ticks, boot ID, one cluster
manifest, and one address reservation before provisioning. Every child receives the exact
`SCYLLA_CCM_RUN_DIR` marker. Commands launch without a shell in a new process group; timeout,
failure, or interruption triggers bounded TERM/KILL cleanup with exit proof. Diagnostics are
transferred before destructive removal and exclude private keys. Failed transfer or removal keeps
ownership and address state for retry or next-process recovery.

[`tests/integration/config.py`](../../tests/integration/config.py) holds one reusable default lease
for the ordinary integration suite and exports its endpoints, topology labels, and CA. The pytest
session hook in [`tests/conftest.py`](../../tests/conftest.py) closes all owned state. The
[`Makefile`](../../Makefile) runs provisioning contracts first and the ordinary suite second in
separate foreground pytest phases so private contracts cannot conflict with the suite lease.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `CCM-REQ-001` | [`cluster_spec.py`](../../tests/testinfra/cluster_spec.py) | [`test_ccm_cluster_spec.py`](../../tests/unit/test_ccm_cluster_spec.py) | `conformant` |
| `CCM-REQ-002` | [`ccm_provisioner.py`](../../tests/testinfra/ccm_provisioner.py) | [`test_authorized_cluster_provides_working_credentials`](../../tests/integration/test_ccm_provisioning.py) | `conformant` |
| `CCM-REQ-003` | [`pool.py`](../../tests/testinfra/pool.py), [`cluster.py`](../../tests/testinfra/cluster.py) | [`test_same_spec_reuses_cluster_with_independent_resource_scopes`](../../tests/integration/test_ccm_provisioning.py) | `conformant` |
| `CCM-REQ-004` | [`cluster.py`](../../tests/testinfra/cluster.py) | [`test_private_https_cluster_can_change_lifecycle_and_topology`](../../tests/integration/test_ccm_provisioning.py) | `conformant` |
| `CCM-REQ-005` | [`pool.py`](../../tests/testinfra/pool.py) | [`test_active_incompatible_and_private_requests_fail_fast`](../../tests/unit/test_ccm_pool.py) | `conformant` |
| `CCM-REQ-006` | [`run_state.py`](../../tests/testinfra/run_state.py), [`ccm_provisioner.py`](../../tests/testinfra/ccm_provisioner.py) | [`test_hard_killed_process_is_recovered_before_reprovisioning`](../../tests/integration/test_ccm_provisioning.py) | `conformant` |

## Test coverage

- [`test_ccm_cluster_spec.py`](../../tests/unit/test_ccm_cluster_spec.py) covers immutable values,
  every security combination, topology limits, canonical keys, reserved aliases, YAML parsing, and
  complete deterministic reuse identity.
- [`test_ccm_pool.py`](../../tests/unit/test_ccm_pool.py) and
  [`test_ccm_cluster.py`](../../tests/unit/test_ccm_cluster.py) cover shared reuse, fail-fast
  incompatibility, unhealthy replacement, private exclusivity, dirty state, bounded resource
  cleanup, idempotent close, lower node limits, and interruption.
- [`test_ccm_run_state.py`](../../tests/unit/test_ccm_run_state.py) covers owner identity,
  cross-process reservations, stale recovery, marked-process termination, malformed-state
  quarantine, safe roots, and atomic-publication interruption.
- [`test_ccm_provisioner.py`](../../tests/unit/test_ccm_provisioner.py) covers command translation,
  TLS, YAML preservation, durable logs, diagnostics, rollback, path/metadata/PID safety, and
  process-group cleanup.
- [`test_ccm_provisioning.py`](../../tests/integration/test_ccm_provisioning.py) proves real-cluster
  reuse and namespace cleanup, enforced authorization, custom-CA HTTPS, YAML overrides, private
  lifecycle/topology changes, and recovery after a hard process exit.
- [`test_ccm_install.py`](../../tests/unit/test_ccm_install.py),
  [`test_ccm_wiring.py`](../../tests/unit/test_ccm_wiring.py), and
  [`test_repository_constraints.py`](../../tests/unit/test_repository_constraints.py) cover pinned
  installation, concurrent repair, two-phase orchestration, shutdown failure reporting, constant
  alignment, and the repository ban on shell-script files.

## Known conformance gaps

None currently known. Malformed or unremovable state remains quarantined instead of being removed
speculatively; this is the safe behavior required by `CCM-REQ-006`.
