# Alternator Client Capability Roadmap

This roadmap tracks the remaining client capability work for the Python
Alternator client. Requirements are written in implementation-independent terms
so each issue has enough context on its own.

## Issue Map

- [#36: Add client capability matrix and deterministic behavior test harness](https://github.com/scylladb/alternator-client-python/issues/36)
- [#33: Add Helper and AsyncHelper lifecycle facade](https://github.com/scylladb/alternator-client-python/issues/33)
- [#32: Plan node health tracking and quarantine behavior](https://github.com/scylladb/alternator-client-python/issues/32)
- [#35: Add explicit routing-scope fallback and topology validation APIs](https://github.com/scylladb/alternator-client-python/issues/35)
- [#34: Wire transport and configuration options into boto clients](https://github.com/scylladb/alternator-client-python/issues/34)
- [#38: Add TLS client certificate and key log file support](https://github.com/scylladb/alternator-client-python/issues/38)
- [#37: Add configurable gzip compression and custom header whitelist callback](https://github.com/scylladb/alternator-client-python/issues/37)
- [#23: Align key-affinity routing semantics for RMW and BatchWriteItem](https://github.com/scylladb/alternator-client-python/issues/23)
- [#39: Coordinate compatibility and release decisions for client capability work](https://github.com/scylladb/alternator-client-python/issues/39)
- [#40: Update documentation, examples, and release notes for client capability work](https://github.com/scylladb/alternator-client-python/issues/40)

## Current Capability Status

Supported or close:

- host-only seeds with one shared port
- sync boto3 client, boto3 resource, async aioboto3 client
- `/localnodes` node discovery
- explicit cluster/datacenter/rack routing scopes and fallback chains
- request-scoped lazy query plans with stable seeded ordering
- gzip request compression
- header optimization
- static Alternator auth, disabled by default
- transport timeout, retry, pool, region, and SDK customizer settings
- TLS server CA and insecure trust-all modes
- TLS client certificate and key log file support
- key route affinity with partition-key cache
- helper lifecycle facade and public inspection methods
- vector search support

Tracked gaps:

- node health tracking and quarantine planning only: [#32](https://github.com/scylladb/alternator-client-python/issues/32)
- configurable compression and header whitelist behavior: [#37](https://github.com/scylladb/alternator-client-python/issues/37)
- key-affinity RMW and BatchWriteItem routing semantics: [#23](https://github.com/scylladb/alternator-client-python/issues/23)
- compatibility and release decisions: [#39](https://github.com/scylladb/alternator-client-python/issues/39)
- documentation, examples, and release notes: [#40](https://github.com/scylladb/alternator-client-python/issues/40)

## Compatibility Decisions

Tracked by [#39](https://github.com/scylladb/alternator-client-python/issues/39).

- Keep current auth behavior: unsigned by default, explicit static credentials
  for signed Alternator requests.
- Keep raw boto credential kwargs working with deprecation warnings until a
  separate removal decision.
- Keep the current convenience default port unless a major-release migration
  explicitly changes it.
- Keep deprecated names such as `AlternatorConfig` and `TlsConfig` working while
  capability changes land.
- Treat routing fallback changes as compatibility-sensitive and document a
  migration path before changing constructor behavior.

## Node Health Constraint

Tracked by [#32](https://github.com/scylladb/alternator-client-python/issues/32).
Related legacy tracking:
[#3](https://github.com/scylladb/alternator-client-python/issues/3).

Node health is planning-only in this roadmap. Do not add node health code, tests,
configuration objects, routing behavior, or default behavior changes unless a
separate future request explicitly authorizes implementation.

## Implementation Order

1. Add capability matrix and deterministic fake Alternator test fixtures:
   [#36](https://github.com/scylladb/alternator-client-python/issues/36).
2. Add helper lifecycle facade and public inspection methods:
   [#33](https://github.com/scylladb/alternator-client-python/issues/33).
3. Keep node health deferred; maintain planning issue only:
   [#32](https://github.com/scylladb/alternator-client-python/issues/32).
4. Add explicit routing fallback and topology validation:
   [#35](https://github.com/scylladb/alternator-client-python/issues/35).
5. Wire timeout, region, pool, and SDK customizer configuration:
   [#34](https://github.com/scylladb/alternator-client-python/issues/34).
6. Add TLS client certificates and key log file support:
   [#38](https://github.com/scylladb/alternator-client-python/issues/38).
7. Add gzip level/custom compressor and custom header whitelist support:
   [#37](https://github.com/scylladb/alternator-client-python/issues/37).
8. Align key-affinity RMW and BatchWriteItem behavior:
   [#23](https://github.com/scylladb/alternator-client-python/issues/23).
9. Record release decisions:
   [#39](https://github.com/scylladb/alternator-client-python/issues/39).
10. Update README, examples, and release notes:
    [#40](https://github.com/scylladb/alternator-client-python/issues/40).

## Final Verification

- `make lint`
- `make test-unit`
- `make test-integration`
- async tests with `[async]` extras installed
- TLS integration tests
- package build check
- example scripts against local Scylla where applicable
