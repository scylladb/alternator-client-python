# Private native interface, version 1

This contract coordinates the native transport, converters and Python facade.
It is private and experimental. Increment `INTERFACE_VERSION` for an incompatible
boundary change. Adding the staged client methods below does not change version 1.

## Available in the package foundation

The extension exports integer `INTERFACE_VERSION = 1` and string `__version__`
from static Python project metadata. `load_extension()` in
`alternator._experimental.rust._native` loads the extension only on explicit use
and requires both the interface and release version to match the base package.
Missing wheels and mismatches raise an actionable `ImportError`. An extension's
own dependency import failure propagates, preserving its original cause.
Ordinary imports, including the experimental package, never load the extension.

The extension supplies diagnostics `_runtime_initialized()`, `_runtime_id()` and
`_runtime_id_async()`. Import and metadata inspection do not initialize Tokio.
The latter two execute on Tokio and return its actual runtime ID as a string;
the async function requires a running Python event loop. They do no network I/O.

`runtime::shared_runtime()` returns a fallible process-wide `&'static Runtime`,
initialized once with the multi-thread Tokio builder and all drivers enabled.
Initialization failures are retained and become `RuntimeError` at the boundary.
The async bridge is explicitly initialized with that same runtime before any
`future_into_py` call. Sync execution releases the GIL with `Python::detach` and
uses that runtime's `block_on`. Async bridging uses
`pyo3_async_runtimes::tokio::future_into_py`, whose future is cancelled when the
Python future is cancelled. Python-owned request data must be converted to owned
Rust values before releasing the GIL or spawning work. No runtime is owned or
shut down by a client. The transport layer adds per-client task tracking, cancellation, loop affinity
and shutdown without replacing this runtime.

## Client boundary to implement in transport and converter changes

Construction and close are exported by the transport implementation. Execution
and its error classes remain reserved for converter and operation changes; no
successful placeholder data operations exist.

| Boundary | Signature and behavior |
| --- | --- |
| Extension construction | `create_client(config: dict[str, object]) -> NativeClient`; `create_client_async(config: dict[str, object]) -> Awaitable[NativeClient]` |
| Data calls | `NativeClient.execute(operation: str, parameters: dict[str, object]) -> dict[str, object]`; `execute_async(operation, parameters) -> Awaitable[dict[str, object]]` |
| Cleanup | `NativeClient.close() -> None`; `close_async() -> Awaitable[None]` |

The facade maps the ten approved Python operation names to their DynamoDB model
names (`GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query`, `Scan`,
`BatchGetItem`, `BatchWriteItem`, `TransactGetItems`, `TransactWriteItems`). The
native boundary consumes ordinary low-level Python dictionaries directly, with
no JSON serialization round trip or Python data SDK call. Binary data remains
bytes/bytearray on input and bytes on output; number strings and list-based sets
remain intact. All response and service-error dictionaries are independently
owned and include the contract's response metadata.

Config is an owned, normalized mapping produced by the facade from the public
Config/Auth types. The transport change freezes and documents its complete key
schema alongside validation tests before exporting construction. Unsupported
fields and values fail before discovery or data network activity. Allowed
callbacks are resolved once by the facade before construction and the results
are frozen for both native and lazy Python admin clients.

Each native client owns its pools, request registry and discovery tasks on the
shared runtime. Calls after close fail with `RuntimeError`. Close is idempotent,
cancels and drains outstanding requests and discovery, and leaves other clients
working. The facade also closes any lazy Python delegate. Async facade clients
bind to the creating Python event loop and reject use on another loop. Cancelling
a Python awaitable cancels its native future; it cannot undo accepted writes.

The converters introduce native `ValidationError` and `ServiceError` exception
classes. ValidationError exposes `report: str` for conversion to Python
`ParamValidationError`; ServiceError exposes `response: dict[str, object]` and
`operation_name: str` for cached-model Python `ClientError` construction without
an admin client. Configuration failures are `ValueError` translated by the
facade to `ConfigurationError`. Unsupported data operations raise
`NotImplementedError`; runtime/closed-client failures raise `RuntimeError`.
Every operation remains unavailable or raises explicitly until implemented.
There is no implicit Python fallback after a native error.

## Python integration to implement in the facade change

The explicit sync factories live under `alternator._experimental.rust` and the
async equivalents under `alternator._experimental.rust.async_client`, matching
the P1 contract factory. Both expose `create_client`, `close_client` and `client`.
Factories call `load_extension()` before constructing native state. Paginators
for query/scan invoke facade data methods. Only the documented lazy admin
allowlist and waiters may create a Python delegate. This document does not add
public extras or expose unfinished factories.


## Frozen construction schema (transport implementation)

`alternator._experimental.rust._config.prepare_config(config, auth=None)` is the
pure Python preflight. Its PreparedConfig supplies fresh `native_config()` and
`delegate_config()` snapshots plus `auth`. Allowed user-agent callbacks resolve
once. Native and lazy admin receive the same frozen PEM contents; private
snapshot files last until `PreparedConfig.close()`. Facades must close it after
any delegate, including failed/cancelled construction. Config/Auth subclasses
are rejected except the explicit deprecated Config/TLS compatibility names.
Preflight requires `parameter_validation=True`; the Python SDK-specific bypass
is rejected before callbacks or TLS snapshots. Native typed validation is always
enabled, so the construction mapping has no validation-bypass setting.

Every key below is required; unknown/missing keys fail with ValueError before
network activity. Values are owned by Rust before GIL release or async spawn.
Schema additions require coordinated preflight/construction tests.

| Keys | Owned value and interpretation |
| --- | --- |
| schema_version | integer 1 |
| seed_hosts, port, scheme | list of host-only names/bare IPs; 1..65535; http/https |
| scopes | nonempty ordered list of (kind, datacenter-or-None, rack-or-None); cluster/datacenter/rack |
| active_interval_ms, idle_interval_ms | positive u64 milliseconds |
| key_affinity, table_pk_attributes | NONE/RMW/ANY_WRITE; copied string-to-string dict |
| access_key_id, secret_access_key, session_token | str or None; key/secret together; optional token; all None disables signing/provider chains |
| region | nonempty string |
| request_gzip, compression_min_bytes, gzip_level | bool; nonnegative usize threshold (64-bit on supported targets); 0..9 |
| response_compression | ordered list of gzip/deflate strings |
| optimize_headers, user_agent | bool; static ASCII header string or None |
| max_attempts | positive u32 integer including initial attempt; STANDARD SDK retries |
| connect_seconds, read_seconds, discovery_seconds | finite seconds, at least 1ns, representable as Rust Duration and a platform monotonic deadline; discovery default 5s |
| system_roots, ca_pem | bool; list of copied PEM bytes; verified nonempty usable root store |
| client_cert_pem, client_key_pem | PEM bytes or None; absent separate key means combined cert/key PEM |

Numeric extraction rejects values outside these native field widths. Python
preflight rejects cyclic fallback chains; scope construction and discovery work
grow with the configured chain. This schema does not guarantee resources for
arbitrarily large configurations. Deadline representability is checked by Rust
before discovery so an overflowing timeout cannot silently disable its timer.

Native HTTP uses owned HTTP/1.1 connection pools; at most 200 idle connections
are retained per authority, with no Python pool tuning. read_seconds bounds the
complete SDK operation, including attempts and retry waits, and each response
read. connect_seconds bounds DNS/TCP/TLS/HTTP handshake. discovery_seconds maps
to upstream's bounded topology/PK discovery API. There is no promise of exact
Python socket-read or retry timing. TLS always verifies chains and hostname.

Rust `client::Client::run` is the sole complete-operation admission/registry seam.
It returns `(SDK output, metadata::OperationMetadata)` or `registry::Closed`.
OperationMetadata owns `attempts: u32` (including attempts without responses)
and `responses: Vec<AttemptResponse>`; it dereferences to the response slice.
Each captured response owns status, original wire header values as byte vectors,
complete wire body and one-based SDK attempt count. Header names are strings;
`request_id()` exposes a UTF-8 request ID when present, including ordinary ASCII
IDs. A native SDK interceptor records every actual attempt in the operation scope
and attaches its number to a private request extension. This preserves correct
RetryAttempts with header optimization, previous connection failures and final
errors without any HTTP response. RetryAttempts is attempts.saturating_sub(1).
Discovery has no operation capture scope. P5 converts these values into its response/error dictionaries.

Close closes admission synchronously, starts one cancellation-safe cleanup task
on the shared runtime, joins upstream shutdown, drains registrations, clears
pools and joins every tracked socket driver. Dropping a client starts the same
cleanup when it observes sole Rust ownership. Concurrent destruction of Rust
clones may not start this task; await explicit close to guarantee shutdown,
registration draining and socket-driver joins. Cancelling a request drops its
SDK future and connection lease, closing that request's socket. Async creation
binds the Python event loop; async methods reject another loop. Raw sync close
can drain after that loop has gone away; the async Python facade retains its
public loop checks.
