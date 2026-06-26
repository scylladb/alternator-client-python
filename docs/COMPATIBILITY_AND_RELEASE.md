# Compatibility And Release Decisions

This document records compatibility and release decisions for the client
capability roadmap tracked by
[#39](https://github.com/scylladb/alternator-client-python/issues/39). Use it
with [RELEASING.md](../RELEASING.md) when preparing releases and with
[FEATURE_PARITY.md](../FEATURE_PARITY.md) when evaluating behavior-changing
roadmap work.

## Scope

These decisions apply to the capability work covering helper lifecycle APIs,
routing fallback, transport configuration, TLS options, compression/header
configuration, and key-affinity behavior. Node health, decommission handling,
and dead-node handling remain planning-only in
[#32](https://github.com/scylladb/alternator-client-python/issues/32) and
[#3](https://github.com/scylladb/alternator-client-python/issues/3).

## Decisions

### Default Port

Keep the convenience default port at `8000`. `Config` continues to require an
explicit `port`, while convenience helpers and builders keep their existing
`8000` default.

Changing this default would be an incompatible behavior change. It requires a
major release, release notes, and migration guidance that tells users to pass
the desired `port` explicitly.

### Authentication Default

Keep request signing disabled by default. Users who need Alternator request
signing should pass `auth=Auth.static_credentials(...)`.

The client-managed authentication API supports explicit static credentials only.
SDK environment, profile, and provider-chain credentials are not used for
Alternator auth.

Raw SDK credential keyword arguments such as `aws_access_key_id` remain accepted
for compatibility and continue to emit deprecation warnings. Do not remove these
kwargs in the same release as the capability work.

### Routing Scope Fallback

Keep existing fallback behavior for default routing-scope constructors:

- `DatacenterScope("dc1")` falls back to `ClusterScope()`.
- `RackScope("dc1", "rack1")` falls back to `DatacenterScope("dc1")` and then
  `ClusterScope()`.

New code should prefer explicit fallback configuration with `fallback=...`,
`with_default_fallback(...)`, or `without_fallback(...)`.

Changing default fallback behavior requires a warning period and a major
release. Release notes must include migration examples for callers that need
cluster fallback, datacenter fallback, or no fallback.

### Node Health

Node health, quarantine behavior, decommission handling, and dead-node handling
are not part of the current implementation scope. Do not add node health code,
tests, config objects, routing behavior, or default behavior changes unless a
separate future request explicitly authorizes implementation.

Future node health behavior should start as opt-in. Making it default behavior
would be compatibility-sensitive and should only happen in a major release with
migration notes.

### Deprecated Names

Keep deprecated compatibility names such as `AlternatorConfig` and `TlsConfig`
working while capability changes land. They should continue to warn and point to
`Config` and `TLS`.

Do not remove deprecated names in the same release as these capability changes.
Any removal requires a separate release decision and a major release.

### Additive Capability APIs

Helper lifecycle APIs, transport configuration, TLS client certificate/key log
settings, compression/header configuration, and key-affinity controls are
additive public API changes as long as existing defaults remain unchanged.

Header optimization, request compression, key-affinity modes, TLS client
certificates, TLS key logs, and SDK config customization remain opt-in. The
client continues to own endpoint routing and authentication signature settings
after SDK config customization.

### Key-Affinity Semantics

Key-affinity behavior is compatibility-sensitive because it affects which node
is tried first for eligible writes. The current behavior keeps normal routing as
the fallback when partition-key metadata is missing, key values are unsupported,
there are no active nodes, there are no eligible batch votes, or batch votes
tie.

Release notes should call out tightened read-modify-write detection and
`BatchWriteItem` preferred-node voting so operators can validate routing
expectations before enabling key affinity in production.

## Versioning Guidance

Use semantic versioning:

- Minor release: additive compatible APIs, new opt-in capabilities, and
  documentation-only compatibility decisions.
- Patch release: compatible bug fixes with no new public capability surface.
- Major release: changed defaults, removed deprecated names, removed legacy
  credential kwargs, changed default routing fallback, changed default auth, or
  default-enabled node health/quarantine behavior.

The capability batch should be released as a minor version unless a separate
maintainer decision adds an incompatible change.

## Release Checklist Additions

Before releasing this capability batch:

- Update README, examples, and release notes for the implemented public API.
- State that node health remains planning-only unless a separate implementation
  has landed.
- Document migration impact for auth, routing fallback, deprecated names, and
  key-affinity behavior.
- Run `make lint`.
- Run `make test-unit`.
- Run integration tests against local Scylla where release validation requires
  real Alternator behavior.
- Run async tests with async extras installed.
- Run TLS integration checks where TLS behavior changed.
- Run the package build check from the release workflow.

Behavior-changing issues and pull requests in this roadmap should link back to
this document or to
[#39](https://github.com/scylladb/alternator-client-python/issues/39).
