# Experimental native backend

`alternator-client-rust-backend` is an independent wheel distribution. It exports
`_alternator_rust` and does not replace the pure `alternator-client` distribution.
Its static Python release version matches `alternator/_version.py`; the internal
Cargo crate version is independent. This package currently supplies interface
version 1 and a shared runtime, with no data transport or client factories yet.
See [INTERFACE.md](INTERFACE.md) for the staged client contract.

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
.venv/bin/python -m pytest native/tests/test_extension.py native/tests/test_startup_cleanup.py
```

Run the Rust core independently of Python or extension-module linking:

```sh
(cd native && cargo test --locked)
(cd native && cargo clippy --locked --all-targets -- -D warnings)
```

The default Cargo feature set excludes PyO3; maturin explicitly enables
`extension`. Commit `Cargo.lock`. The Rust transport dependency will be added in
the transport change at the merged shutdown revision; the current baseline
revision is recorded in Cargo metadata, without resolving that dependency here.

The selected PyO3 0.27.2 supports CPython 3.10–3.14 and is pinned together with
pyo3-async-runtimes 0.27.0. Upstream references:
[PyO3 supported interpreters](https://pyo3.rs/v0.27.2/),
[PyO3 0.27 release notes](https://github.com/PyO3/pyo3/releases/tag/v0.27.0),
[PyO3 extension and ABI features](https://pyo3.rs/v0.27.2/features.html),
[maturin configuration](https://www.maturin.rs/config.html), and
[async bridge runtime initialization](https://docs.rs/pyo3-async-runtimes/0.27.0/pyo3_async_runtimes/tokio/fn.init_with_runtime.html).

Tokio 1.54.1 carries a small downstream worker-startup cleanup patch, applied
through one Cargo-root override. The original archive, exact two-file patch,
MIT license, per-file checksums and removal criteria are documented in
[vendor/README.md](vendor/README.md). Wheels retain Tokio's license, attribution
and patch provenance. Core and Linux fault probes cover failed construction;
portable runtime/interface tests run on every supported interpreter/platform.
Linux fault probes require a C compiler and run only in disposable subprocesses.
