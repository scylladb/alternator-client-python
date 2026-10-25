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
changes, including the final verified merged P0 Git pin. Regenerate context for
any source/lock/wheel change. The current manifest, lock and graph name the exact
declared P0 Git revision `549c1b43a281f0b6c0644dfcbcbabb24ddafb7c1`; that source
identity alone does not establish final merged-pin or release qualification.

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

Implemented caller plumbing and a local wheel pass do not establish final
qualification. The retained host run covers Linux x86_64 CPython 3.13 with a
`manylinux_2_38` wheel; it does not qualify the required `manylinux_2_28` floor,
all 20 interpreter/platform cells or live Scylla behavior. Those require separate
exact-head CI evidence. The PR now targets Python main after #150 and uses its
PyO3/pyo3-async-runtimes 0.29.0 bridge. The transport rejects unsuccessful
`GET /localnodes` responses, and a held-socket lifecycle test proves the configured
discovery deadline independently of the read timeout. Before marking the draft
ready, a compatible final merged Rust pin still requires maintainer approval and
renewed source/lock/graph/startup qualification.

## Controlled final pin update

The current `P4_FULL_MANIFEST` anchor binds the manifest declaring the Git
revision above. After maintainer approval and parent verification of the actual
P0 merge/green main, update the declared Git revision and final Git-source lock
while preserving the single audited Tokio identity. Review the manifest diff,
replace this one P4 manifest anchor with the resulting exact SHA256, and keep
the separate P2 minimal and upstream-full anchors. Do not append a permissive
fallback or derive ownership from the new observed FD delta. If the upstream
manifest/ownership source changes, renew that source audit too. Then build a
fresh normal-wheel context, verify its upstream Cargo source names the merged
revision, and rerun the positive startup probes and all provenance/FD negative
controls. Context schema 1, extension interface 1, Python release 2.0.1 and
internal Cargo package version 0.1.0 are independent of this pin-only update.
