# Native backend compatibility contract, version 1

This is the executable scope for issues [#131](https://github.com/scylladb/alternator-client-python/issues/131)
and [#132](https://github.com/scylladb/alternator-client-python/issues/132). The
native backend is staged: shared fixtures check the Python clients, and the
native wheel supplies configuration, lifecycle, typed conversion and error
interfaces. Native operation execution and Python facades remain follow-up
work. The independent `alternator-client-rust-backend` distribution will
provide `_alternator_rust` wheels. The base package stays pure Python. Public
`native` and `async-native` extras will require that distribution at the exact
same Python release version; `async-native` also includes async Python
dependencies. Base and `async` installation remain independent of Rust.

The native surface is explicit and private: sync `create_client`, `close_client`
and `client` under `alternator._experimental.rust`, with equivalents under
`alternator._experimental.rust.async_client`. There is no backend selection or
Python fallback after a native data failure. All ten data methods execute
conversion, serialization, signing, routing, compression, retries, HTTP and
response parsing in Rust. The facade may use cached Python service models to
construct modeled exceptions, without constructing an admin client.

## Operations and parameters

[`tests/contract/manifest.json`](../tests/contract/manifest.json) is the versioned,
complete operation/parameter/response/error manifest. It records input and
output shape names and every reachable nested member, required field, scalar
type, enum and model constraint, including recursive AttributeValue shapes.
`test_manifest_covers_complete_model_graph` detects service-model drift. A model
upgrade requires an intentional manifest review; regenerating it does not
establish native support for new members.

| Python method | Native operation | Legacy forms retained |
| --- | --- | --- |
| `get_item` | GetItem | AttributesToGet |
| `put_item` | PutItem | Expected, ConditionalOperator |
| `update_item` | UpdateItem | AttributeUpdates, Expected, ConditionalOperator |
| `delete_item` | DeleteItem | Expected, ConditionalOperator |
| `query` | Query | AttributesToGet, KeyConditions, QueryFilter, ConditionalOperator |
| `scan` | Scan | AttributesToGet, ScanFilter, ConditionalOperator |
| `batch_get_item` | BatchGetItem | RequestItems[].AttributesToGet |
| `batch_write_item` | BatchWriteItem | PutRequest / DeleteRequest |
| `transact_get_items` | TransactGetItems | Get members and ordered responses |
| `transact_write_items` | TransactWriteItems | ConditionCheck / Put / Update / Delete |

All modeled parameters in the manifest are in scope, including Select, index
names, segmented scans, consumed capacity, collection metrics, expressions,
return values on failed conditions, transaction request tokens, pagination and
unprocessed batch entries. Select is a Query/Scan parameter. PartiQL, vector
operations, resources, arbitrary `boto_kwargs`, event hooks and exact boto3 client
identity are excluded. Unsupported native operations raise NotImplementedError;
unknown arguments, missing required arguments and wrong argument types raise
`botocore.exceptions.ParamValidationError` before data network activity.

Native typed SDKs may reject malformed AttributeValue unions earlier than the
Python backend or service. Exact validation text, validation timing for malformed
values, JSON bytes/key ordering, retry timing and SDK-added headers are not
compatibility promises. Service-side expression/transaction semantics still
need the three-node Scylla integration suite: this fake peer supplies responses,
not a DynamoDB interpreter.

## Values, responses and errors

Low-level AttributeValue dictionaries remain low-level dictionaries. Binary
request members accept bytes/bytearray; parsed B/BS members are bytes. N/NS
values stay strings, preserving spelling, scale and exponent, with no float or
Decimal coercion. SS/NS/BS remain lists. Nested M/L, booleans, nulls, empty
strings/blobs/maps/lists, absent Item and an explicitly empty Item are distinct.
Unsupported high-level Python objects require explicit validation failure.

Fixtures in `tests/contract/cases.py` are fresh independent copies and include
pagination keys, unprocessed keys/items, capacity/index metrics, ordered
transaction responses, conditional failure Items, cancellation reasons, modeled
and unknown service errors. Errors remain ClientError-compatible, with modeled
exception classes where available, Error.Code/Message, operation_name and
ResponseMetadata (HTTPStatusCode, original HTTPHeaders, RequestId and
RetryAttempts). Metadata belongs to its individual response. Python error
dictionaries retain only body members declared by their service-model error
shape. Native error dictionaries retain body members and message casing except
protocol `__type`, which is normalized into Error.Code, and reserved envelope
collisions. Dictionary-valued `Error` and `ResponseMetadata` retain unrelated
nested keys through a shallow merge. Generated Error.Code/Message take
precedence; metadata reserves HTTPStatusCode, HTTPHeaders, RequestId,
RetryAttempts and MaxAttemptsReached, even when genuine optional values are
absent. Body HTTPHeaders are never merged with genuine headers. Non-dictionary
values at the two envelope names are replaced by generated dictionaries;
overwritten originals remain in raw `.body`. Modeled binary Item and
CancellationReasons values remain decoded Python values. Native exception
context also preserves the original wire body and headers for the facade.
Header lookup is case-insensitive; retain server header values even when HTTP
decompression changes the exposed body bytes.
Compare semantic responses and normalize only nondeterministic metadata, such
as generated request IDs and Date. Do not remove empty or absent members during
comparison.

## Configuration and construction

The manifest classifies every current Config and TLS field. Native defaults
need not duplicate Python HTTP implementation details:

| Setting | Mapping / rejection |
| --- | --- |
| Seeds, port, scheme | Frozen host-only seeds, shared port, HTTP or verified HTTPS |
| Routing scope | Built-in cluster/datacenter/rack and explicit built-in fallback chains; custom classes/subclasses rejected |
| Polling | Active/idle milliseconds map to native refresh intervals |
| Key affinity | NONE/RMW/ANY_WRITE and frozen explicit key map; native DescribeTable discovery and native retry policy; nondefault custom backoff rejected |
| Auth and region | Disabled or static credentials, including session token; no implicit provider chain; signing region defaults to us-east-1 |
| Compression | NONE/GZIP request body, threshold and gzip level; gzip/deflate response negotiation/decompression |
| Header policy | Optimization bool accepted; custom whitelist/callback rejected |
| User agent | Static string/None or existing construction-time callback resolved once, frozen for native and Python delegate |
| Retries | STANDARD only; max_attempts includes the initial request; legacy/adaptive rejected |
| Timeouts | Connect timeout, bounded native request deadline from read_seconds, discovery request deadline; no exact socket-read equivalence |
| Python pool limit | Default 200 accepted with native connection-pool defaults; nondefault tuning rejected |
| Parameter validation | Default `True` accepted with native typed validation; reject `False` because bypass is Python SDK-specific |
| TLS trust | Verified system and/or custom PEM CAs; fail construction if no usable roots |
| Mutual TLS | PEM certificate chain and optional separate key; verification remains enabled |
| Advanced TLS | Reject trust_all_certificates, disabled hostname checks, key logging and nondefault session-cache settings |

`tests/contract/config_cases.py` provides valid Python configurations that native
construction must reject with ConfigurationError before any data **or discovery**
network activity. These exclusions do not restrict existing Python clients.
Allowed construction callbacks run once, with frozen results shared by both
components; no per-request Python callbacks are permitted.

The native client owns per-client pools and a cancellable request registry on
one shared Tokio runtime. Sync work releases the GIL. Async clients bind to the
creating event loop; cancellation drops the native future without undoing writes
already accepted by the server. Idempotent close rejects new work, cancels and
drains active requests and discovery, releases pools and closes any lazy Python
delegate, while other clients keep working. Query/scan paginators invoke native
facade methods. Waiters and an explicit admin allowlist may use lazy Python
clients. Failed construction must clean up partial resources.

Only GIL-enabled CPython 3.10–3.14 on manylinux_2_28 x86_64, macOS Intel/ARM64 and
Windows MSVC x64 are in the wheel matrix. Linux ARM64, free-threaded Python,
subinterpreters and inherited clients after fork are excluded.

## Running and extending the contracts

Ordinary `make test-unit` runs both Python sync and async contracts. Targeted:

```sh
uv run pytest tests/unit/test_backend_contract.py tests/unit/test_programmable_server.py
uv run pytest tests/unit/test_backend_contract.py --contract-backend=python-sync
# Once the explicit native extra is installed and rebuilt:
uv run pytest tests/unit/test_backend_contract.py --contract-backend=rust-sync --contract-backend=rust-async
```

Requesting a missing native backend fails; it never silently skips or falls back.
BackendFactory.client is an async context manager exposing `raw` and async
`call(method, **kwargs)`. This test adapter moves sync execution to a worker
thread so concurrent backend tests can share one event loop.

`FakeAlternatorServer` in `tests/fake_server.py` retains the existing GET fixture
API and supports concurrent HTTP/1.1 GET/POST, optional server SSLContext
(including client certificate verification), exact-path GET FIFO scripts and
per-operation/per-table POST FIFO scripts. `script_get(path, responses, fallback=...)`
matches the full path including its query; exhaustion returns HTTP 404 unless a
persistent fallback is supplied. POST tables match before operation-only scripts;
POST exhaustion returns HTTP 400 without a fallback.

Both backends discover topology through `GET /localnodes`. DC scopes use `dc`,
rack scopes use `dc` and `rack`, and query values are URL-encoded. An unscoped
response describes the responder's datacenter; cluster discovery must union
responses from seeds/known nodes rather than assume one seed exposes all DCs.
`set_localnodes` supplies a persistent list for one exact query. `set_topology`
builds scoped IP lists from `(address, datacenter, rack)` rows; its unscoped route
uses the first local row's datacenter. Unknown scopes return 404 unless explicitly
scripted. It never installs topology system-table Scan handlers.
`set_partition_key` still serves HASH-key DescribeTable metadata. GET scripts can
supply malformed bodies, empty lists, HTTP errors, disconnects or held discovery
I/O to exercise configured fallback chains and cancellation. Existing Python
discovery exhaustion behavior remains unchanged. Native lifecycle tests require
an explicitly rebuilt wheel containing the corrected upstream discovery; old
system-table wheels do not provide current acceptance evidence.

Capture includes original request header spelling, wire bytes, decoded gzip JSON
and monotonically assigned connection IDs. `wait_for_requests` (with optional
exact `path` filtering) and response
`entered`/`release` events synchronize bounded tests without sleeps. Concurrent
held responses permit lifecycle/cancellation and pool-reuse assertions. Stop
interrupts held requests and idle keep-alive sockets. Tests verify server
behavior independently, including custom CA trust, hostname rejection and mTLS
certificate enforcement. Later native tests must prove Python validation,
serialization and transports are bypassed, exercise GIL/event-loop progress,
wrong-loop use, cancellation/close races, retry/deadline handling and repeated
creation/cleanup, in addition to passing these initial semantic contracts.

## Conversion implementation status (#134)

All ten input/output model graphs, including legacy forms, have native typed
converters. Rust validates request structure before GIL release and preserves
binary values, numeric string spelling, ordered list sets and empty
containers. Unknown parameters and malformed unions include a parameter path.
SDK optional members preserve response absence; compact markers from the final
wire body supply presence for SDK-defaulted scalars and required nested batch
members. Bulk attribute contents are skipped during presence parsing; JSON
scanning and a second decompression of compressed responses remain. Modeled
maximums are left to the service while client minimum/type/native-width checks
remain. Rust response decoding supports gzip and both deflate forms while
retaining original metadata and wire context. The Python error adapter uses
cached models and creates no admin client.

This is conversion/error qualification only. It does not expose execute methods
or claim end-to-end native support. The existing exact-head core qualification
and all 20 interpreter/platform wheel cells remain required before PR readiness;
final dependency-pin qualification stays in #133.
