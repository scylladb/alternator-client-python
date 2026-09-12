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
compression at `request-created` before signing, response negotiation and header filtering at
`before-send` after signing, and response decoding before DynamoDB protocol parsing. The same
registration function is used by
[`client.py`](../../alternator/client.py) and [`async_client.py`](../../alternator/async_client.py).

## Lifecycle and concurrency

Compression handlers are created once with client construction and capture immutable scalar or
tuple-derived settings. They own no threads, sessions, or shutdown work. Request compression runs
on each newly created SDK attempt, so retries start from the SDK request body rather than mutating
shared global state. Compression completes into a local byte string before the request is changed.

DynamoDB responses are non-streaming SDK outputs. Botocore and aiobotocore provide a fully buffered
body to `before-parse`, where the same synchronous decode handler is used for blocking and
non-blocking clients. On the asynchronous path, aiobotocore completes the transport read before
invoking that hook; the compression handler does not wrap the source, start independent work, or
intercept transport errors or cancellation. It either replaces the buffered body once and returns
to the parser, or raises a decompression error. Once synchronous decoding begins, it has no await
or other cancellation point. This implementation therefore has no explicit demand API or
asynchronous decompression subscription to close or cancel.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `COMP-REQ-001` | [`RequestCompressionConfig` and `_normalize_response_compression`](../../alternator/config.py) | [`TestAlternatorConfig.test_default_values` and compression builder tests](../../tests/unit/test_config.py) | `gap` |
| `COMP-REQ-002` | [`create_compression_handler`](../../alternator/core/compression.py) | [`TestCompressionHandler`](../../tests/unit/test_compression.py), [`TestRequestCompression`](../../tests/integration/test_request_compression.py) | `gap` |
| `COMP-REQ-003` | [`create_compression_handler`](../../alternator/core/compression.py) | — | `gap` |
| `COMP-REQ-004` | [response request and decode handlers](../../alternator/core/compression.py) | [`TestResponseCompressionRequestHandler` and `TestResponseCompressionDecodeHandler`](../../tests/unit/test_response_compression.py), synchronous [`TestResponseCompression`](../../tests/integration/test_response_compression.py) | `gap` |
| `COMP-REQ-005` | [`_decode_gzip_response` and `_decode_deflate_response`](../../alternator/core/compression.py) | [`test_invalid_gzip_response_raises` and `test_invalid_deflate_response_raises`](../../tests/unit/test_response_compression.py) | `gap` |
| `COMP-REQ-006` | [`create_response_compression_decode_handler`](../../alternator/core/compression.py), [`_create_async_client_with_manager`](../../alternator/async_client.py) | — | `gap` |
| `COMP-REQ-007` | [`_register_alternator_handlers`](../../alternator/core/handlers.py) | [`test_signed_request_url_and_compressed_body_are_final_before_signing`](../../tests/unit/test_request_lifecycle.py), [`test_headers_and_compression_share_wire_whitelist`](../../tests/integration/test_request_compression.py) (request compression only) | `gap` |
| `COMP-REQ-008` | [`_register_alternator_handlers`](../../alternator/core/handlers.py), [`_create_async_client_with_manager`](../../alternator/async_client.py) | [`TestRequestCompression` and `TestAsyncRequestCompression`](../../tests/integration/test_request_compression.py) (request path only) | `gap` |

`COMP-REQ-006` applies to the asynchronous client. For non-streaming DynamoDB operations,
aiobotocore awaits the transport body before `before-parse`, and the handler does not alter that
buffering strategy. Only the additional rules for an explicit demand-driven protocol are
inapplicable because this client exposes no such protocol. The remaining error, cancellation,
single-emission, and completion guarantees are not directly verified for asynchronous response
decompression. In particular, the synchronous decode step has no cancellation point once it
begins, and there is no async decode, error, or cancellation test evidence, so the requirement is
recorded as a gap rather than assumed conformant.

## Test coverage

- [`test_config.py`](../../tests/unit/test_config.py) covers compression defaults, builder
  propagation, algorithm validation for responses, and gzip-level bounds.
- [`test_compression.py`](../../tests/unit/test_compression.py) covers request thresholds, gzip
  round trips, body types, header updates, compression levels, and empty bodies. The named
  size-benefit test uses a three-byte body with a ten-byte threshold, so it exits at the threshold
  check and does not exercise the size-benefit branch.
- [`test_response_compression.py`](../../tests/unit/test_response_compression.py) covers negotiation
  order, duplicate suppression, gzip and deflate decoding, stale-header removal, unsupported
  encodings, and outright corrupt bodies. It does not cover valid deflate data followed by trailing
  bytes or another stream.
- [`test_request_lifecycle.py`](../../tests/unit/test_request_lifecycle.py) verifies on one
  first-attempt request preparation that routing and request compression finish before SigV4
  signing. It does not exercise compression failure or replay on retry, and it does not enable
  response negotiation.
- [`test_request_compression.py`](../../tests/integration/test_request_compression.py) inspects sync
  and async request-compression bytes on the wire and the request-compression interaction with
  signing and header optimization.
- [`test_response_compression.py`](../../tests/integration/test_response_compression.py) verifies
  sync end-to-end gzip and deflate response parsing when the server supports them. Async response
  and corrupt-response integration paths do not have direct coverage.

No direct test covers compressor failure and replay, an eligible incompressible request, duplicate
wire `Content-Encoding` fields, or asynchronous response negotiation and decoding.

## Known conformance gaps

- `COMP-REQ-001`: `RequestCompressionConfig.algorithm` is not runtime-validated, so a value other
  than `NONE` or `GZIP` can survive configuration. Response entries are type-checked, but duplicate
  algorithms remain in the stored configuration and are removed only when the wire header is
  built. Response disablement is represented by an empty sequence rather than a dedicated disabled
  configuration.
- `COMP-REQ-002`: The threshold comparison is inclusive, but eligible bodies are compressed only
  when the gzip result is smaller than the original. The generic contract also requires
  incompressible non-empty bodies at or above the threshold to be transmitted as gzip.
- `COMP-REQ-003`: The handler computes a complete gzip byte string before mutating the request,
  which structurally avoids sending a partial encoder result. However, no test injects a compressor
  failure or retries a compressed request, so the failure and replay guarantees are not fully
  established.
- `COMP-REQ-004`: Negotiation replaces only a missing, blank, or `identity` `Accept-Encoding` value;
  a custom value such as `br` is preserved instead of overridden. The decode handler is not given
  the configured algorithm set, so enabling only gzip still decodes a deflate response. It also
  does not recognize a single supported token with parameters such as `gzip; level=1`. On the
  asynchronous path, the pinned aiobotocore transport overwrites the prepared value with
  `Accept-Encoding: identity` immediately before sending. It also collapses repeated response
  header fields into a single dictionary value before the decode hook, so an ambiguous repeated
  `Content-Encoding` can be mistaken for one supported coding and decoded.
- `COMP-REQ-005`: `_decode_deflate_response` uses one-shot `zlib.decompress`, which accepts the
  first valid zlib stream without rejecting trailing bytes. A valid deflate-encoded JSON body
  followed by arbitrary bytes or another compressed stream is therefore treated as successfully
  decoded, its encoding metadata is removed, and the first decoded value is exposed to protocol
  parsing.
- `COMP-REQ-006`: The buffered asynchronous path has no demand API, but the requirement's general
  async error, cancellation, single-emission, and completion guarantees still apply. No direct
  async response-decompression evidence verifies them, and synchronous decoding cannot observe
  cancellation once it starts.
- `COMP-REQ-007`: Response negotiation runs at `before-send`, after SigV4 signing, so
  `Accept-Encoding` is not finalized before signing and is absent from `SignedHeaders`. A user
  request hook can also leave a custom `Accept-Encoding` value that advertises an encoding this
  client will not decode. Compressed HTTP `500`, `502`, `503`, and `504` responses, decompression
  failures after response receipt, and their future node-health accounting also lack composition
  coverage.
- `COMP-REQ-008`: Request compression uses the shared handler in both client forms, but response
  behavior diverges at the transport boundary. The synchronous client sends the configured
  `Accept-Encoding`, while aiobotocore replaces it with `identity`. The synchronous header
  container preserves repeated `Content-Encoding` fields as an ambiguous comma-joined value, while
  aiobotocore retains only the last field and can cause the shared handler to decode it.
