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
shut down by a client. This foundation does not yet implement per-client task
tracking, cancellation, loop affinity or shutdown.

## Client boundary to implement in transport and converter changes

These symbols are reserved by this contract and are **not exported yet**. There
are no successful placeholder data operations. The transport change introduces
construction/close; converter and operation changes introduce execution.

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
