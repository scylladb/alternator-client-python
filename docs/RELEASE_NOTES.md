# Capability Release Notes

Use this file as the release-note source for the capability roadmap tracked in
[FEATURE_PARITY.md](../FEATURE_PARITY.md). Keep
[docs/COMPATIBILITY_AND_RELEASE.md](COMPATIBILITY_AND_RELEASE.md) as the
authority for compatibility decisions.

## Next Major Release

### Additive APIs

- Added `Helper` and `AsyncHelper` lifecycle facades for callers that need one
  owner for clients/resources plus topology diagnostics.
- Added explicit routing fallback controls with `fallback=...` and
  `without_fallback(...)`.
- Added topology validation helpers for configured datacenter/rack scopes.
- Added transport configuration for SDK retries, connect/read timeouts,
  connection pool size, region placeholder, and SDK config customization.
- Added TLS client certificate, client key, and key log file settings.
- Added configurable gzip compression level and callback-based header whitelist
  additions.
- Updated key-affinity routing semantics for read-modify-write operations and
  `BatchWriteItem` preferred-node voting.

### Compatibility Notes

- Request signing remains disabled by default.
- Alternator request signing continues to use explicit static credentials via
  `auth=Auth.static_credentials(...)`.
- Raw SDK credential kwargs remain supported for compatibility, but emit
  deprecation warnings.
- The convenience default port remains `8000`; pass `port=...` explicitly if
  your deployment uses a different port.
- Datacenter and rack constructors now stay constrained by default. Use
  `fallback=...` when broader routing is desired.
- Deprecated names such as `AlternatorConfig` and `TlsConfig` remain available
  and continue to warn. Prefer `Config` and `TLS` in new code.
- Node health, quarantine behavior, decommission handling, and dead-node
  handling remain planning-only.

### Behavior Notes

- `RMW` key-affinity mode applies only to writes that require a prior item
  state. `BatchWriteItem` does not use affinity in `RMW` mode.
- `ANY_WRITE` key-affinity mode selects a preferred node for single-item writes.
  For `BatchWriteItem`, valid put/delete entries vote for their preferred node.
  Tied votes, missing partition-key metadata, unsupported key values, no active
  nodes, or no eligible votes fall back to normal routing.
- `sdk_config_customizer` can adjust supported SDK config kwargs, but the client
  still owns endpoint routing and reapplies auth-managed signature settings.
- TLS key logs contain traffic decryption material and should only be used in
  protected debugging environments.

### Migration Steps

- Replace `AlternatorConfig` with `Config` and `TlsConfig` with `TLS` in new or
  updated code.
- Replace raw SDK credential kwargs with
  `auth=Auth.static_credentials(access_key_id, secret_access_key)`.
- Replace implicit topology fallback assumptions with explicit `fallback=...`
  when requests may broaden from rack to datacenter or cluster scope.
- Preload partition-key metadata with `table_pk_map` when key affinity should be
  active on the first request for a table.
- Review timeout configuration as per-attempt SDK settings, not whole-operation
  deadlines.

### Versioning Expectation

The capability batch is compatible additive work and should be released as a
minor version unless a separate maintainer decision adds an incompatible change.
Changed defaults, removed deprecated names, removed legacy credential kwargs,
changed routing fallback defaults, changed auth defaults, or default-enabled node
health/quarantine behavior require a major release.
