# Experimental native backend

`alternator-client-rust-backend` is an independent wheel distribution. It
exports `_alternator_rust` and does not replace the pure `alternator-client`
distribution. Its static Python release version matches
`alternator/_version.py`; the internal Cargo crate version is independent.
This package supplies interface version 1, the shared runtime, validated
private native constructors and per-client HTTP transport/lifecycle. Typed
request/response conversion and reserved error interfaces are available; data
execution remains staged for the operation changes. See
[INTERFACE.md](INTERFACE.md) for the frozen configuration and client contract.

Only GIL-enabled CPython 3.10–3.14 is supported. Release wheels target
manylinux_2_28 x86_64, macOS Intel/ARM64 and Windows MSVC x64. Wheels are specific
to each interpreter; neither abi3 nor free-threaded wheels are enabled. Release
jobs must publish wheels only. There is no public native extra yet.

## Build and test explicitly

Ordinary base/async development never builds this package. Native development
requires Rust 1.94.1, pinned by `rust-toolchain.toml`, and maturin 1.15.0, pinned
by the PEP 517 build requirements. Rebuild after each native source edit:

```sh
uv venv .venv
uv pip install --python .venv/bin/python -e '.[dev,async]' maturin==1.15.0
(cd native && ../.venv/bin/maturin build --release --locked --interpreter ../.venv/bin/python --out ../dist/native)
uv pip install --python .venv/bin/python dist/native/*.whl
.venv/bin/python -m pytest native/tests/test_extension.py native/tests/test_client_lifecycle.py
```

Run the Rust core independently of Python or extension-module linking:

```sh
(cd native && cargo test --locked)
(cd native && cargo clippy --locked --all-targets -- -D warnings)
```

The default Cargo feature set excludes PyO3; maturin explicitly enables
`extension`. Commit `Cargo.lock`. The Rust transport dependency must stay pinned
to a verified merged shutdown revision. Its SDK/Smithy compatibility versions
are explicitly constrained; update the graph deliberately with core transport,
configuration and lifecycle checks. Native HTTP owns all socket driver tasks,
and close joins discovery, requests and drivers while retaining the shared
runtime. No data method falls back to Python.

The selected PyO3 0.29.0 supports CPython 3.10–3.14 and is pinned together with
pyo3-async-runtimes 0.29.0. Upstream references:
[PyO3 supported interpreters](https://pyo3.rs/v0.29.0/),
[PyO3 0.29 release notes](https://github.com/PyO3/pyo3/releases/tag/v0.29.0),
[PyO3 extension and ABI features](https://pyo3.rs/v0.29.0/features.html),
[maturin configuration](https://www.maturin.rs/config.html), and
[async bridge runtime initialization](https://docs.rs/pyo3-async-runtimes/0.29.0/pyo3_async_runtimes/tokio/fn.init_with_runtime.html).

Tokio 1.54.1 carries a small downstream worker-startup cleanup patch, applied
through one Cargo-root override. The original archive, exact two-file patch,
MIT license, per-file checksums and removal criteria are documented in
[vendor/README.md](vendor/README.md). Wheels retain Tokio's license, attribution
and patch provenance. Core and Linux fault probes cover failed construction;
portable runtime/interface tests run on every supported interpreter/platform.
Linux fault probes require a C compiler and run only in disposable subprocesses.
Their exact normal-wheel build context and source-bound signal-global accounting
are described in [tests/STARTUP.md](tests/STARTUP.md).

The native dependency and baseline metadata pin the deadline integration in
[Rust #158](https://github.com/scylladb/alternator-client-rust/pull/158) at
`d2044849bd3b0c100d18541b01df4a1a1eac3139`, the verified merged revision with
successful upstream main CI. It integrates the reviewed
[Rust #156](https://github.com/scylladb/alternator-client-rust/pull/156) deadlines
onto current main's shared discovery ownership and rack-affinity views. The lock
update changes only the upstream Git identity; the SDK graph and single patched
Tokio 1.54.1 dependency remain unchanged.

The startup audit confirms the upstream manifest still requests Tokio `full`.
The signal ownership source anchors, upstream-full anchor and separate minimal
anchor remain unchanged; only the native manifest anchor is renewed. Qualification
must use a fresh ordinary wheel and its compiled Tokio fingerprint, followed by
all startup failure probes and negative controls described in
[tests/STARTUP.md](tests/STARTUP.md).

Both manifest entries and the lock now name the merged Rust SHA, and the native
manifest anchor has been renewed. The fresh ordinary host wheel/context artifacts
are recorded in [tests/STARTUP.md](tests/STARTUP.md). Before the dependent Python
PR becomes ready, require exact-head CI, including the existing 20-cell wheel
matrix and aggregate gate; host qualification alone does not satisfy that gate.
Merged-pin qualification receipts remain at `stage: interface` with
`release_qualified: false`. This change adds conversion and error mapping; data
operations remain staged for later issues. Python APIs, native
interface/configuration schema 1 and all package versions remain unchanged.

## Converter checks

The converter uses checked-in Rust generated from the compatibility manifest and
the locked SDK source. Regenerate after an intentionally reviewed model change:

```sh
python3 native/scripts/generate_converters.py
python3 native/scripts/generate_converters.py --check
(cd native && cargo clippy --locked --all-targets --features conversion-tests -- -D warnings)
(cd native && cargo test --locked --features conversion-tests --test conversion --test lifecycle)
```

The `conversion-tests` feature links embedded Python with extension-module
linking disabled. It exercises the shared fixtures and every reachable model
member without Python SDK serialization or transport. Default Rust core tests
still exclude PyO3. Maturin continues to build only the `extension` feature;
converter tests do not change the normal-wheel Tokio feature fingerprint.
