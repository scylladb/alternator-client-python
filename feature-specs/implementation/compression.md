# Compression implementation

This document connects the [compression specification](../compression.md) to the Python
implementation in this repository. It is informative: deviations recorded here do not weaken the
generic contract.

## Public API

[`CompressionAlgorithm`, `ResponseCompression`, and `RequestCompressionConfig`](../../alternator/config.py)
define the request and response algorithms, request threshold, and gzip level. `Config` exposes
`request_compression` and `response_compression`; both directions are disabled by default.

[`AlternatorConfigBuilder.with_compression(...)`](../../alternator/config.py) enables request
compression. `with_response_compression(...)` enables one or more accepted response encodings, and
`without_response_compression()` disables response compression. The synchronous client, synchronous
resource, and asynchronous client all consume the same `Config` fields.

## Internal architecture

[`create_compression_handler`](../../alternator/core/compression.py) is a botocore
`request-created` handler. It converts string, bytes, or byte-array bodies to bytes, checks the
configured byte threshold, creates a complete gzip result, and then replaces the body and updates
`Content-Encoding` and `Content-Length`. It currently retains the original body when gzip is not
smaller, even if the threshold was met.

[`create_response_compression_request_handler`](../../alternator/core/compression.py) computes an
ordered, de-duplicated `Accept-Encoding` value and runs at `before-send`.
[`create_response_compression_decode_handler`](../../alternator/core/compression.py) runs at
`before-parse`, buffers gzip or deflate decoding, replaces the response body, and removes stale
`Content-Encoding` and `Content-Length` fields after successful decoding.

[`_register_alternator_handlers`](../../alternator/core/handlers.py) installs routing and request
compression before signing, response negotiation before header filtering, and response decoding
before DynamoDB protocol parsing. The same registration function is used by
[`client.py`](../../alternator/client.py) and [`async_client.py`](../../alternator/async_client.py).

## Lifecycle and concurrency

Compression handlers are created once with client construction and capture immutable scalar or
tuple-derived settings. They own no threads, sessions, or shutdown work. Request compression runs
on each newly created SDK attempt, so retries start from the SDK request body rather than mutating
shared global state. Compression completes into a local byte string before the request is changed.

DynamoDB responses are non-streaming SDK outputs. Botocore and aiobotocore provide a fully buffered
body to `before-parse`, where the same synchronous decode handler is used for blocking and
non-blocking clients. Consequently this implementation has no publisher/subscriber demand API or
asynchronous decompression subscription to close or cancel.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `COMP-REQ-001` | [`RequestCompressionConfig` and `_normalize_response_compression`](../../alternator/config.py) | [`TestAlternatorConfig.test_default_values` and compression builder tests](../../tests/unit/test_config.py) | `gap` |
| `COMP-REQ-002` | [`create_compression_handler`](../../alternator/core/compression.py) | [`TestCompressionHandler`](../../tests/unit/test_compression.py), [`TestRequestCompression`](../../tests/integration/test_request_compression.py) | `gap` |
| `COMP-REQ-003` | [`create_compression_handler`](../../alternator/core/compression.py) | [`test_signed_request_url_and_compressed_body_are_final_before_signing`](../../tests/unit/test_request_lifecycle.py) | `conformant` |
| `COMP-REQ-004` | [response request and decode handlers](../../alternator/core/compression.py) | [`TestResponseCompressionRequestHandler` and `TestResponseCompressionDecodeHandler`](../../tests/unit/test_response_compression.py), [`TestResponseCompression`](../../tests/integration/test_response_compression.py) | `gap` |
| `COMP-REQ-005` | [`_decode_gzip_response` and `_decode_deflate_response`](../../alternator/core/compression.py) | [`test_invalid_gzip_response_raises` and `test_invalid_deflate_response_raises`](../../tests/unit/test_response_compression.py) | `conformant` |
| `COMP-REQ-006` | [`create_response_compression_decode_handler`](../../alternator/core/compression.py) | — | `not-applicable` |
| `COMP-REQ-007` | [`_register_alternator_handlers`](../../alternator/core/handlers.py) | [`test_signed_request_url_and_compressed_body_are_final_before_signing`](../../tests/unit/test_request_lifecycle.py), [`test_headers_and_compression_share_wire_whitelist`](../../tests/integration/test_request_compression.py) | `gap` |
| `COMP-REQ-008` | [`_register_alternator_handlers`](../../alternator/core/handlers.py), [`_create_async_client_with_manager`](../../alternator/async_client.py) | [`TestAsyncRequestCompression`](../../tests/integration/test_request_compression.py) | `conformant` |

`COMP-REQ-006` is not applicable to this implementation: DynamoDB responses are buffered before
the decode hook, and neither supported client form exposes a demand-driven response body or an
asynchronous decompression stream.

## Test coverage

- [`test_config.py`](../../tests/unit/test_config.py) covers compression defaults, builder
  propagation, algorithm validation for responses, and gzip-level bounds.
- [`test_compression.py`](../../tests/unit/test_compression.py) covers request thresholds, gzip
  round trips, body types, header updates, compression levels, empty bodies, and the current
  size-benefit behavior.
- [`test_response_compression.py`](../../tests/unit/test_response_compression.py) covers negotiation
  order, duplicate suppression, gzip and deflate decoding, stale-header removal, unsupported
  encodings, and corrupt bodies.
- [`test_request_lifecycle.py`](../../tests/unit/test_request_lifecycle.py) verifies routing and
  compression finish before SigV4 signing.
- [`test_request_compression.py`](../../tests/integration/test_request_compression.py) inspects sync
  and async request bytes on the wire and the interaction with signing and header optimization.
- [`test_response_compression.py`](../../tests/integration/test_response_compression.py) verifies
  sync end-to-end gzip and deflate response parsing when the server supports them. Async response
  and corrupt-response integration paths do not have direct coverage.

## Known conformance gaps

- `COMP-REQ-001`: `RequestCompressionConfig.algorithm` is not runtime-validated, so a value other
  than `NONE` or `GZIP` can survive configuration. Response entries are type-checked, but duplicate
  algorithms remain in the stored configuration and are removed only when the wire header is
  built. Response disablement is represented by an empty sequence rather than a dedicated disabled
  configuration.
- `COMP-REQ-002`: The threshold comparison is inclusive, but eligible bodies are compressed only
  when the gzip result is smaller than the original. The generic contract also requires
  incompressible non-empty bodies at or above the threshold to be transmitted as gzip.
- `COMP-REQ-004`: Negotiation replaces only a missing, blank, or `identity` `Accept-Encoding` value;
  a custom value such as `br` is preserved instead of overridden. The decode handler is not given
  the configured algorithm set, so enabling only gzip still decodes a deflate response. It also
  does not recognize a single supported token with parameters such as `gzip; level=1`.
- `COMP-REQ-007`: A user request hook can leave a custom `Accept-Encoding` value that advertises an
  encoding this client will not decode. Retryable compressed-error responses, decompression
  failures after response receipt, and their future node-health accounting also lack composition
  coverage.
