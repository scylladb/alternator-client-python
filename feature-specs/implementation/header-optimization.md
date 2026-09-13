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

The filter is not the last stage that can mutate the outgoing header map. A later `before-send`
hook can add headers after it, and transport serialization happens after all event handlers. The
synchronous urllib3 transport supplies a default `User-Agent` when one is absent. The asynchronous
path replaces `Accept-Encoding` with `identity` before calling aiohttp, which can then supply
default `Accept` and `User-Agent` headers when they are absent.

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
header mutation path, but their transports perform different header synthesis afterward.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `HEAD-REQ-001` | [`HeaderOptimizationConfig`](../../alternator/config.py), [`create_header_filter_handler`](../../alternator/core/headers.py) | [`TestHeaderFilterHandler`](../../tests/unit/test_headers.py), [sync and async prepared-request tests](../../tests/integration/test_header_optimization.py) | `gap` |
| `HEAD-REQ-002` | [`BASE_REQUIRED_HEADERS`, `AUTH_HEADERS`, and `compute_header_whitelist`](../../alternator/core/headers.py) | [`TestComputeHeaderWhitelist`](../../tests/unit/test_headers.py), [`test_header_filter_preserves_every_sigv4_signed_header`](../../tests/unit/test_request_lifecycle.py) | `gap` |
| `HEAD-REQ-003` | [`HeaderOptimizationConfig`](../../alternator/config.py), [`compute_header_whitelist`](../../alternator/core/headers.py) | [`test_custom_whitelist_added` and callback tests](../../tests/unit/test_headers.py), [`test_build_with_header_optimization`](../../tests/unit/test_config.py) | `gap` |
| `HEAD-REQ-004` | [`_register_alternator_handlers`](../../alternator/core/handlers.py) | [`TestHeaderFilterWithHandlerRegistration`](../../tests/unit/test_headers.py), [request lifecycle ordering tests](../../tests/unit/test_request_lifecycle.py) | `gap` |
| `HEAD-REQ-005` | [`create_header_filter_handler`](../../alternator/core/headers.py) | [`TestHeaderFilterHandler`](../../tests/unit/test_headers.py) | `conformant` |
| `HEAD-REQ-006` | [`_register_alternator_handlers`](../../alternator/core/handlers.py), [`_create_async_client_with_manager`](../../alternator/async_client.py) | [sync and async prepared-request tests](../../tests/integration/test_header_optimization.py) | `gap` |

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
  request compression, signing, and header filtering together on a prepared request immediately
  before transport send.

## Known conformance gaps

- `HEAD-REQ-001`: Filtering runs on the prepared request, not on the final serialized header map.
  Hooks registered later can add non-whitelisted fields, and urllib3 or aiohttp can synthesize
  headers after all `before-send` handlers. Consequently, the implementation does not filter every
  header that reaches the wire. The cited integration tests snapshot prepared requests and do not
  observe transport-generated headers.
- `HEAD-REQ-002`: The built-in base set treats `Accept-Encoding` as always required rather than
  conditional on response compression and omits `User-Agent` from the computed required set even
  when reporting is enabled. It also includes `X-Amz-Security-Token` whenever authentication is
  enabled, including long-lived static credentials with no session token. The last handler still
  adds the configured user agent to the prepared request, but callbacks observe an incomplete and
  over-inclusive required set.
- `HEAD-REQ-003`: A caller whitelist is treated as additions to the computed defaults, so an empty
  or incomplete set is accepted instead of rejected. `None` or empty entries are not proactively
  validated, validation does not run while optimization is disabled, and direct construction of
  `HeaderOptimizationConfig` can retain a mutable set; only the builder and the final filter create
  immutable snapshots.
- `HEAD-REQ-004`: Routing, request compression, response negotiation, and signing run before
  filtering, but final user-agent handling is registered after it. Any later `before-send` hook can
  likewise add a header after filtering. Transport serialization is later still: when user-agent
  reporting is disabled, the handler removes the prepared `User-Agent`, but urllib3 and aiohttp each
  reintroduce their own default value. Thus enabled user-agent reporting bypasses the effective
  whitelist and disabled reporting does not remain disabled on the wire.
- `HEAD-REQ-006`: The shared handler gives both forms the same filtering behavior at the prepared
  request stage, but their final header maps are not equivalent. aiohttp adds `Accept` when it is
  absent, and with user-agent reporting disabled urllib3 and aiohttp synthesize different default
  `User-Agent` values. The asynchronous transport also replaces `Accept-Encoding` with `identity`
  after event processing. The cited tests stop before these transport-specific mutations.
