# Capability Release Notes

Use this file as the release-note source for the current capability batch.

## Next Major Release

### Additive APIs

- Added `Session` and `AsyncSession` lifecycle facades for callers that need one
  owner for clients/resources plus topology diagnostics.
- Added explicit routing fallback controls with `fallback=...` and
  `without_fallback(...)`.
- Added topology validation helpers for configured datacenter/rack scopes.
- Added transport configuration for SDK retries, connect/read timeouts,
  connection pool size, region placeholder, TLS client certificates, and
  User-Agent customization.
- Added Alternator-specific `User-Agent` control with `Config.user_agent`.
  By default, requests send the Alternator Python client identity; callers can
  pass `None` to remove the wire header, a string to set a final value, or a
  callback to wrap/add to the default identity.
- Added TLS client certificate, client key, and key log file settings.
- Added configurable gzip compression level and callback-based header whitelist
  additions.
- Added opt-in gzip and deflate response compression decoding.
- Updated key-affinity routing semantics for read-modify-write operations and
  `BatchWriteItem` preferred-node voting.

### API Notes

- Request signing remains disabled by default.
- Alternator request signing continues to use explicit static credentials via
  `auth=Auth.static_credentials(...)`.
- Boto-style credential kwargs such as `aws_access_key_id` remain accepted by
  the boto3-shaped factories. Do not combine them with `auth=...`.
- The convenience default port remains `8000`; pass `port=...` explicitly if
  your deployment uses a different port.
- Datacenter and rack constructors now stay constrained by default. Use
  `fallback=...` when broader routing is desired.
- The public config names are `Config` and `TLS`; pre-release aliases and the
  fluent config construction API are not part of the release API.
- Node health, quarantine behavior, decommission handling, and dead-node
  handling remain planning-only.

### Behavior Notes

- `RMW` key-affinity mode applies only to writes that require a prior item
  state. `BatchWriteItem` does not use affinity in `RMW` mode.
- `ANY_WRITE` key-affinity mode selects a preferred node for single-item writes.
  For `BatchWriteItem`, valid put/delete entries vote for their preferred node.
  Tied votes, missing partition-key metadata, unsupported key values, no active
  nodes, or no eligible votes fall back to normal routing.
- The client owns the SDK config object, endpoint routing, auth-managed
  signature settings, TLS client certificate settings, retries, timeouts, and
  final wire `User-Agent` handling.
- TLS key logs contain traffic decryption material and should only be used in
  protected debugging environments.

### Review Steps

- Use direct `Config(...)` construction with typed nested config objects.
- Prefer `auth=Auth.static_credentials(access_key_id, secret_access_key)` when
  Alternator auth should be explicit in application code.
- Replace implicit topology fallback assumptions with explicit `fallback=...`
  when requests may broaden from rack to datacenter or cluster scope.
- Preload partition-key metadata with
  `KeyRouteAffinityConfig(table_pk_attributes=...)` when key affinity should be
  active on the first request for a table.
- Review timeout configuration as per-attempt SDK settings, not whole-operation
  deadlines.

### Versioning Expectation

This release defines the initial public API for the current capability batch.
Future changes that remove public names, change defaults, change auth behavior,
or default-enable node health/quarantine behavior should be treated as breaking
changes.
