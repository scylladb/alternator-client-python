# Tokio startup cleanup patch

This directory carries official Tokio 1.54.1 at commit
`b53b2001ed2ccfd9b87e32a0c6da1a1eebdc80b8` under its unchanged MIT license.
The official archive contains 581 files. Only `src/runtime/blocking/pool.rs` and
`src/runtime/scheduler/multi_thread/worker.rs` are changed by the recorded patch.
The Cargo-root override makes direct and transitive consumers use one Tokio.

Partial worker creation otherwise leaves running workers waiting during build
unwind. First-worker failure also strands a queued startup task and its driver.
The patch keeps worker cores reachable until launch succeeds, closes the failed
scheduler and drains startup work when no blocking worker can consume it.
Application initialization separately catches the construction panic inside its
OnceLock so Python receives the same retained, cause-bearing RuntimeError.

## Verify and regenerate

Download `https://static.crates.io/crates/tokio/tokio-1.54.1.crate` to an external
path. Its SHA256 must be
`b52fd3255258d26fac7264ab5a76b53c13367136dc011d3d6ad4f8ed070396a3`.
Extract its `tokio-1.54.1/` directory as `native/vendor/tokio/` and apply
`patch -p1 --input=../tokio-1.54.1-startup-cleanup.patch` from that directory.
Do not copy a populated Cargo registry/cache. Preserve every archive file byte
for byte except the two patched files. Git attributes disable text conversion.

From `native/`, run:

```sh
python scripts/verify_vendor.py --archive /external/path/tokio-1.54.1.crate --cargo
cargo test --locked
../.venv/bin/python -m pytest tests/test_extension.py tests/test_startup_cleanup.py
```

`tokio-provenance.json` records archive/patch/upstream commit and every pristine
and patched file digest. The verifier rejects extra/missing/changed files and
multiple Tokio identities. It also independently pins the SHA256 of the approved
patched-file manifest: `281e96a7b8647ba097d17296e77feb1d7538c4ec13810f4371e074a141d358f1`.
The digest uses UTF-8 lines `SHA256`, two spaces, POSIX path, and LF, sorted by
path. Updating source bytes together with the editable provenance hashes cannot
silently certify drift from the audited patch. Update this trusted anchor only
after independently reconstructing and reviewing the official archive plus the
approved patch; a regenerated manifest alone is insufficient. The patch SHA256 is
`d0856157d59a7e9bf4df0578089fb7d58dbacb8907df8cb2c5c61df00657a324`.
The wheel includes the unchanged MIT license and NOTICE attribution; build source
archives include this source and provenance. Native publication remains wheels
only.

The Linux regression interposer runs only in bounded child processes. It tests
first/middle/last worker failure, worker joins, driver FDs, callback release and
subsequent healthy timers/TCP/blocking operations, plus actual installed-wheel
RuntimeError retention. This is injected thread-creation failure evidence, not
macOS/Windows failure qualification. Other platforms run portable core and wheel
checks. The supported constructor uses stable features from ordinary CPython;
OOM/abort, hostile custom hooks, unstable timers and entry from an already
entered Tokio runtime are outside that exposed construction path.

Remove the override, vendored tree and patch only after a verified official
Tokio release passes these same cleanup and Python-boundary regressions. Update
provenance and dependency review together; do not silently rebase the patch.
