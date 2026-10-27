# Linux startup cleanup qualification

These are developer probes for the ordinary installed wheel. They require a C
compiler and Rust for the standalone core harness; they add no compiler
requirement to package installation. The production extension, shared runtime,
worker policy and audited Tokio vendor bytes are unchanged.

P0 requests Tokio `full`. On Unix, `signal` **or** `process` enables Tokio's
signal-internal driver. The audited `signal/registry.rs` stores `OsExtraData` in
a static `OnceLock`; `signal/unix.rs` creates one original Unix stream pair.
Each runtime's signal driver duplicates the receiver and owns that duplicate.
Only the two original process-global endpoints may persist; the duplicate,
reactor, waker and every other runtime descriptor must close.

`startup_context.py build` creates a fresh target directory used only for one
normal release wheel build. It binds the exact wheel/extension hashes to its
actual compiled Tokio fingerprint, current native source/lock and independently
anchored source contracts. A dev or all-features graph, stale target directory,
observed descriptor delta or arbitrary environment flag cannot establish the
expected pair count. The accepted P2 minimal manifest requires zero pairs; the
accepted P4/upstream-full manifests require exactly one. Unknown contracts fail
closed. Renew the source audit and manifest anchors after relevant dependency
changes, including verified merged P0 Git pins. Regenerate context for
any source/lock/wheel change. The current manifest and lock name the exact
merged P0 Git revision `d2044849bd3b0c100d18541b01df4a1a1eac3139`; that source
identity alone does not establish installed-wheel or release qualification.

For local Linux qualification after preparing the ordinary Python environment:

```sh
uv run --with maturin==1.15.0 python native/tests/startup_context.py build \
  --interpreter .venv/bin/python --output /tmp/alternator-startup-wheel
uv pip install /tmp/alternator-startup-wheel/*.whl
ALTERNATOR_STARTUP_CONTEXT=/tmp/alternator-startup-wheel/startup-context.json \
ALTERNATOR_STARTUP_WHEEL=/tmp/alternator-startup-wheel/EXACT_WHEEL_FILENAME.whl \
  .venv/bin/python -m pytest native/tests
```

Keep the wheel and context together. Verification and installed-wheel probes
need no Rust feature query: the parent test verifies external build context,
source and exact installed binary before passing the expectation to the child.
A missing or mismatched context fails. Normal wheel builds can strip through
Cargo's release profile; accounting uses no private symbols or backtrace names.
The independent integration audit also exercised stripped copies and approved
P2 strict-zero wheel controls.

The child-local C interposer is armed only around the **first** synchronous
shared-runtime construction. It records successful socketpair descriptor
numbers, devices and inodes, and requires the audited Unix stream/nonblocking/
close-on-exec/protocol-zero shape. Before exemption, the externally proven count
must match exactly. A second pair, an extra `/dev/null` descriptor, or a duplicate
of a permitted endpoint is rejected at every first/middle/last worker failure.
Baseline descriptors must keep their identities. Cached sync and async errors
must remain identical; later asyncio descriptors receive no exemption. Worker
creation/join accounting and bounded thread-reclamation checks still apply.
Context controls reject changed counts, compiled features, upstream proof,
source, wheel hashes and coordinated alternate-source substitutions.

The standalone Rust harness first builds/drops a local current-thread driver to
initialize process globals before its descriptor baseline. That starts no
workers. The first multi-thread build remains faulted, with original cache,
callback-drop, thread/descriptor cleanup and healthy subsequent-runtime checks.

These assertions qualify ordinary Linux startup failure for the exact audited
source/build. Socketpair shape alone cannot prove ownership universally. Future
source that suppresses/replaces the known global pair while leaking a same-shaped
pair inside the armed interval needs renewed audit. This is not qualification
for arbitrary embeddings, OOM, descriptor exhaustion, signal delivery or signal
handler registration.

## CI caller integration

The Linux core job implements the source-bound startup caller:

1. It builds the normal wheel with `startup_context.py build`, retaining the
   generated wheel and `startup-context.json` from the same fresh build.
2. Core collection requires `--startup-context PATH` and passes its absolute
   path plus the exact wheel as `ALTERNATOR_STARTUP_CONTEXT` and
   `ALTERNATOR_STARTUP_WHEEL` to the installed-wheel test subprocess.
3. It copies context into core evidence and binds its SHA256 and wheel hash into
   the core receipt/aggregate verifier. Exact source/run and complete native
   collection checks, installed base `[async]`/native origins, compiler probes
   and no-skip assertions remain required. Collection includes vendor,
   installed-wheel interface, startup and client lifecycle cases.
4. Caller/helper fixtures and qualification tests cover the required context
   argument and receipt. No optional context fallback is supported.

## Deadline integration source audit

The dependency and baseline metadata now name the verified merged revision of
[Rust #158](https://github.com/scylladb/alternator-client-rust/pull/158),
`d2044849bd3b0c100d18541b01df4a1a1eac3139`, with successful upstream main CI.
Its manifest SHA256 is still
`9b0ddba41cf7851cf16696874b4aa8dfbaa8acfeb76bafd96e3b4fff507f5228`:
Tokio `full` remains enabled. The lock diff changes only the upstream Git source;
Tokio remains the single path-patched 1.54.1 identity.

The renewed ownership audit reads `signal/registry.rs`'s static `OnceLock<Globals>`,
`signal/unix.rs`'s original stream pair, the runtime signal driver's receiver
duplication and ownership, and the feature gates in `runtime/driver.rs` and
`macros/cfg.rs`. All five `SIGNAL_SOURCE` hashes still match. The worker-startup
patch and its provenance are unchanged. Only `P4_FULL_MANIFEST` is replaced with
the exact new native manifest hash. The P2 minimal and upstream-full anchors
remain separate and unchanged. No observed descriptor delta determines ownership.

A fresh normal-wheel context must bind this manifest/lock, the compiled Tokio
features and the exact installed binary. Run the first/middle/last worker failure
probes, their FD/thread controls, context provenance controls and standalone Rust
startup harness against that wheel. Keep the original wheel and context together.
A host Linux wheel pass does not qualify the manylinux_2_28 floor or the complete
20-cell interpreter/platform matrix; require exact-head CI and its aggregate.
The existing `stage: interface` and `release_qualified: false` remain unchanged.

## Local qualification artifacts

The 2026-10-27 Linux x86_64 CPython 3.13.5 build uses Rust 1.94.1 and maturin
1.15.0, with a fresh target used only for the normal release wheel. Its compiled
Tokio fingerprint contains `full`, `signal` and `process`; the independent source
contracts require exactly one process-global signal pair.

- Wheel: `alternator_client_rust_backend-2.0.1-cp313-cp313-manylinux_2_38_x86_64.whl`
- Wheel SHA256: `26c70f525f770942983034226d4ceae3f9f97bb2d867df8d8bb8e7f90eaf143f`
- Context SHA256: `1252a2c5c0e120d57d3f7731cd842f6996edb0ed9c178ee86232d92cb727b6e0`
- Native manifest SHA256: `d7c10e71f2197612bddce04236e032ee284aa78382cb452db1859ca59cb91d7b`
- Native lock SHA256: `c2697650147726000f30a417d5bb5bb02b3f244912a5926bae579f9eec3fae4c`

The local artifact directory is `/tmp/dk-pr158-finalization-evidence/normal-wheel`;
it contains the original wheel, `startup-context.json` and the fresh build
log/fingerprint. Core qualification must install these exact bytes outside the
checkout and retain `core.json` plus the complete `native.xml` report. CI creates
its own fresh context and wheels for its source/run; the local hashes identify
only this host build. The host wheel does not establish the manylinux_2_28 floor
or release qualification.

## Controlled merged pin update

Rust #158 merged at `d2044849bd3b0c100d18541b01df4a1a1eac3139`; its main
checks passed. This update replaces target main's Rust baseline
`549c1b43a281f0b6c0644dfcbcbabb24ddafb7c1` with that merged revision. The full
range integrates awaitable discovery shutdown, separates discovery ownership
from node views, and updates rack-affinity/query-plan routing. It retains the
separate discovery deadlines and classifies invalid discovery timeouts as
discovery configuration errors. Qualification here covers configuration,
discovery deadlines, cancellation and shutdown. It does not establish the full
affinity/query-plan request-routing contract, which remains part of later
data-operation qualification.

For future pin updates, verify the actual Rust merge and successful main CI,
then update both the dependency revision and baseline metadata plus the final
Git-source lock. Preserve
the single audited Tokio identity. Review the manifest diff, replace the one P4
manifest anchor with its exact SHA256, and retain the separate P2 minimal and
upstream-full anchors. Renew the upstream/ownership audit if those sources change.
Build a fresh ordinary wheel/context and rerun positive probes, all provenance/FD
negative controls and exact-head CI before marking the dependent Python PR ready.
Context schema 1, extension interface 1, Python release 2.0.1 and internal Cargo
version 0.1.0 are unchanged. No release publication is part of this update.
