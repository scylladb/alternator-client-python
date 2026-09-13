# Header optimization implementation

This document connects the [header-optimization specification](../header-optimization.md) to the
Python implementation in this repository. It is informative: deviations recorded here do not
weaken the generic contract.

## Public API

[`HeaderOptimizationConfig`](../../alternator/config.py) exposes `enabled`, `whitelist`, and
`whitelist_callback`. [`HeaderWhitelistContext`](../../alternator/config.py) supplies the full
client configuration, authentication and request-compression flags, and the computed required set
to callbacks. [`AlternatorConfigBuilder.with_header_optimization(...)`](../../alternator/config.py)
enables filtering and freezes a directly supplied set when the builder is used.

[`Config.user_agent`](../../alternator/config.py) controls the final user-agent value, while
[`Auth`](../../alternator/config.py) and [`apply_auth`](../../alternator/core/auth.py) determine
whether the SDK signs requests. Both synchronous and asynchronous client factories install header
optimization from the same configuration object.

## Internal architecture

[`compute_header_whitelist`](../../alternator/core/headers.py) unions the built-in base set,
authentication headers, request-compression headers, a caller set, and callback results, then
returns a `frozenset`. This additive custom-set behavior differs from the generic custom-whitelist
validation contract.

[`create_header_filter_handler`](../../alternator/core/headers.py) snapshots lowercase names and,
for each prepared request, deletes only headers outside that set. It also parses SigV4
`SignedHeaders` from `Authorization` and protects every signed field, including signer extensions
not known when the whitelist was computed. Allowed header keys and values are not rebuilt.

[`create_user_agent_header_handler`](../../alternator/core/headers.py) replaces the SDK user agent
with the configured value or removes it when disabled.
[`_register_alternator_handlers`](../../alternator/core/handlers.py) registers response-compression
negotiation before filtering and registers user-agent handling last. Endpoint routing and request
compression already ran at `request-created`, and SigV4 signing already produced its final signed
header list before the `before-send` filter runs.

Topology polling uses the independent fetchers in [`_http.py`](../../alternator/_http.py); it is not
wrapped by the DynamoDB event handlers.

## Lifecycle and concurrency

The effective lowercase whitelist is computed once during SDK client or resource construction.
The optional callback also runs at that point, not per request. The resulting filter closure holds
an immutable snapshot and otherwise has no shared mutable state; per-attempt signed-header
protection is derived from that attempt's request.

Filtering mutates only the prepared request's header mapping at `before-send`. It owns no transport,
thread, completion object, or shutdown hook, so client close and cancellation remain SDK behavior.
The synchronous boto3 and asynchronous aiobotocore clients use this same handler registration and
header mutation path.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `HEAD-REQ-001` | [`HeaderOptimizationConfig`](../../alternator/config.py), [`create_header_filter_handler`](../../alternator/core/headers.py) | [`TestHeaderFilterHandler`](../../tests/unit/test_headers.py), [sync and async integration tests](../../tests/integration/test_header_optimization.py) | `conformant` |
| `HEAD-REQ-002` | [`BASE_REQUIRED_HEADERS`, `AUTH_HEADERS`, and `compute_header_whitelist`](../../alternator/core/headers.py) | [`TestComputeHeaderWhitelist`](../../tests/unit/test_headers.py), [`test_header_filter_preserves_every_sigv4_signed_header`](../../tests/unit/test_request_lifecycle.py) | `gap` |
| `HEAD-REQ-003` | [`HeaderOptimizationConfig`](../../alternator/config.py), [`compute_header_whitelist`](../../alternator/core/headers.py) | [`test_custom_whitelist_added` and callback tests](../../tests/unit/test_headers.py), [`test_build_with_header_optimization`](../../tests/unit/test_config.py) | `gap` |
| `HEAD-REQ-004` | [`_register_alternator_handlers`](../../alternator/core/handlers.py) | [`TestHeaderFilterWithHandlerRegistration`](../../tests/unit/test_headers.py), [request lifecycle ordering tests](../../tests/unit/test_request_lifecycle.py) | `gap` |
| `HEAD-REQ-005` | [`create_header_filter_handler`](../../alternator/core/headers.py) | [`TestHeaderFilterHandler`](../../tests/unit/test_headers.py) | `conformant` |
| `HEAD-REQ-006` | [`_register_alternator_handlers`](../../alternator/core/handlers.py), [`_create_async_client_with_manager`](../../alternator/async_client.py) | [sync and async wire tests](../../tests/integration/test_header_optimization.py) | `conformant` |

## Test coverage

- [`test_headers.py`](../../tests/unit/test_headers.py) covers required-set composition, callback
  additions, immutable computed results, case-insensitive filtering, signed-header discovery,
  missing header maps, and final user-agent behavior.
- [`test_request_lifecycle.py`](../../tests/unit/test_request_lifecycle.py) exercises a real botocore
  event pipeline and proves a custom SigV4-signed header survives late filtering and the configured
  user agent is applied after filtering.
- [`test_config.py`](../../tests/unit/test_config.py) covers the disabled default and builder
  propagation of direct and callback-based header settings.
- [`test_header_optimization.py`](../../tests/integration/test_header_optimization.py) inspects sync
  and async prepared requests, including custom removal, custom retention, authentication, and user
  agent behavior.
- [`test_request_compression.py`](../../tests/integration/test_request_compression.py) verifies
  request compression, signing, and header filtering together on a prepared wire request.

## Known conformance gaps

- `HEAD-REQ-002`: The built-in base set omits required `Connection`, treats `Accept-Encoding` as
  always required rather than conditional on response compression, and omits `User-Agent` from the
  computed required set even when reporting is enabled. The last handler still adds the configured
  user agent to the wire, but callbacks observe an incomplete and over-inclusive required set.
- `HEAD-REQ-003`: A caller whitelist is treated as additions to the computed defaults, so an empty
  or incomplete set is accepted instead of rejected. `None` or empty entries are not proactively
  validated, validation does not run while optimization is disabled, and direct construction of
  `HeaderOptimizationConfig` can retain a mutable set; only the builder and the final filter create
  immutable snapshots.
- `HEAD-REQ-004`: Routing, compression, response negotiation, and signing run before filtering, but
  final user-agent handling is registered after it. Enabled user-agent reporting therefore bypasses
  the effective whitelist instead of being present for final filtering. Disabled reporting is still
  removed and not reintroduced.
