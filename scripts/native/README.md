# Native wheel CI

The ordinary CI workflow calls `native.yml` for the exact pull-request head or
push SHA, in addition to the existing Python lint, unit and Scylla jobs. The
reusable workflow builds all 20 GIL-enabled CPython 3.10–3.14 wheels, crossing:

| Wheel platform | Rust target | Runner |
| --- | --- | --- |
| `manylinux_2_28_x86_64` | `x86_64-unknown-linux-gnu` | Ubuntu 24.04, pinned manylinux image |
| `macosx_11_0_x86_64` | `x86_64-apple-darwin` | macOS 15 Intel |
| `macosx_11_0_arm64` | `aarch64-apple-darwin` | macOS 14 ARM64 |
| `win_amd64` | `x86_64-pc-windows-msvc` | Windows 2022 x64 |

The explicit macOS deployment target is 11.0 for both architectures. This is a
wheel compatibility floor, not a claim that native transport qualification has
already run on every macOS release. Wheels must have interpreter and ABI tags
matching `cp310` through `cp314`; abi3, free-threaded, universal2, unexpected
deployment tags and wrong architectures fail metadata inspection. The build uses
the committed Rust toolchain and Cargo lock with maturin 1.15.0 release builds.

Each cell installs its exact native wheel and the same base wheel into a fresh
venv in a temporary directory outside the checkout. Installation accepts wheels
only and disables the pip cache. Both installation and smoke run with a PATH
containing only the venv scripts (plus Windows System32). The child checks that
`cargo` and `rustc` cannot be found. It uses Python isolated mode and verifies the
installed base and binary module locations, versions, interface, lazy runtime,
shared sync/async runtime identity, cancellation of bridge tasks, loop cleanup,
and continued runtime availability. Ordinary base import must not load native.
The Rust core separately checks formatting, Clippy and task cancellation without
shutting down the process-wide runtime.

The Linux core job also builds a current native wheel and runs all Python tests
under `native/tests` in a separate fresh environment outside the checkout. The
core command requires the exact built base wheel with `--base`; the workflow
builds and inspects it before core qualification. The environment installs that
base wheel with async dependencies, the exact native wheel, pytest and the async
plugin. It copies only the shared `tests` fixture package into the environment;
production base/extension imports must resolve under that environment, with no
checkout production path. It collects the complete native suite, including
client lifecycle cases, so later operation/facade cases can join without
collection errors or skipped async cases. This developer check retains Rust and
requires `cc`, `cargo`, `rustc`, and Linux `/proc` before it can start. The C
compiler builds a child-local thread-creation fault
interposer; package installation itself does not invoke or require that compiler.
The installed-wheel probes fail startup at the first, middle and last worker,
check stable cached sync/async failures and absence of leaked threads/file
descriptors, and the standalone Rust probe proves a later runtime can work.

Core qualification requires all 45 vendor CLI regression cases (15 each under
normal Python, `-O`, and `PYTHONOPTIMIZE`), all four installed-wheel interface
cases, and all four startup cases. It verifies the canonical anchored 581-file
Tokio source and one Cargo Tokio identity using the native toolchain directory.
Every collected native test must pass: missing cases, skips, duplicate cases,
errors and failures invalidate the JUnit proof even if pytest exits zero. Future
native test files are included by directory collection. The core job retains
`core.json`, `startup-context.json`, the original core wheel and the complete
`native.xml` report, bound to the source SHA, original run, wheel/context hashes
and vendor provenance hash. This is separate from all 20 package cells, which
still install with Rust absent on PATH.

## Reusable workflow inputs

- `source-commit` is a required full 40-character commit SHA. Every job asserts
  this exact checkout and refuses tracked modifications.
- `artifact-prefix` defaults to `native`, and namespaces base, cell and complete
  artifacts when the workflow is called more than once in the same Actions run.
- `source-bundle-artifact` defaults to empty. A release caller can provide the
  name of a **same-run** artifact containing `release-source.bundle`, following
  the existing release preparation convention. The jobs first check out caller
  tooling, verify the bundle, require its advertised HEAD to match the requested
  SHA, fetch it and detach at that SHA. This establishes source identity only.
  The release caller must first authenticate the publisher signature and verify
  the deterministic prepared history against its pinned baseline using reviewed
  release tooling. There is no arbitrary run or repository download input.
- `attest` defaults to false for pull requests. Ordinary CI enables it for
  pushes to main/master. The final matrix job signs only after the core and all
  20 wheel cells succeed and their retained bytes pass aggregation. Build and
  test jobs have no OIDC signing permission. The caller must grant
  `contents: read`, `id-token: write` and `attestations: write` to the workflow
  call. No native job has publishing credentials or long-lived signing secrets.

The `${artifact-prefix}-core` artifact and required aggregate argument
`--core-evidence DIRECTORY` are part of the P10/P11 integration seam. Those
callers must retain the proof and authenticate its trusted-build attestation
before accepting it. The host-tagged core wheel is qualification-only and must
remain separate from published packages: the release inventory still contains
exactly 20 native wheels, one pure base wheel and one base sdist. It is not a
twenty-first native release wheel.

## Evidence and qualification stages

Each cell artifact contains its original wheel, `SHA256SUMS`, `build.json`,
`install.json`, `provenance.json`, and diagnostic `cargo-metadata.json`. Build
receipts use the coordinated release shape: `filename`, `sha256`, `commit`, and original Actions `run_id` (all strings).
Provenance also records the Rust compiler, maturin, Rust target, lock and project
metadata hashes, base wheel hash, installed Python/dependency versions, workflow
identity, run attempt, runner architecture and OS. Mac deployment targets are
preserved in the wheel filename and WHEEL metadata.

The `Native matrix complete` gate runs with `always()` and requires successful
core and matrix job conclusions, core wheel/report proof for this source/run, and
exactly one receipt for every supported cell. It rechecks original wheel bytes,
filename/metadata tags, checksums, source, run, versions, provenance and identical
base wheel hashes. Missing, duplicate,
failed, skipped, cancelled or inconsistent cells fail. The `native-complete`
artifact contains the full build/install inventory and `release_qualified: false`.

P3 receipts explicitly have `stage: interface`. They omit `native_request`,
per-client `cleanup`, and `extras`; they cannot pass P11's release evidence gate.
P10 extends the install seam with both `native` and `async-native` in separate
fresh environments, actual native requests and complete per-client cleanup once
the facades exist. It must preserve exact wheel hashes, source/run identity,
isolation and matrix completeness, and emit the final P11 installation schema.
No data operation is stubbed or treated as successful by this smoke. Trusted
release consumers must authenticate GitHub attestations for original wheel and
receipt hashes, workflow/repository/run identity and successful conclusions;
these JSON receipts alone are not cryptographic proof of provenance.

## Provenance validation contract

`source_provenance()` reads the verified checkout's native project, lock hash
and toolchain, plus base dependencies, using standard-library TOML.
Aggregation therefore needs Python 3.11+, as pinned in the reusable workflow.
The source/build/install commands still support Python 3.10+. Validation does
not compile code, run Cargo, import installed packages or fetch dependencies.

`validate_provenance()` requires exactly the version-2 fields emitted by
`record()`. Schema, interface stage, build receipt, slot and version must match
the installation and pinned source; target/runner must match that cell. Compiler
release/host and maturin must match source pins. The lock and project hashes
must match exact source-derived bytes: LF on Linux/macOS, or LF/checkout-CRLF on
Windows. Other hashes are rejected. Windows newline conversion is not permission
to accept a regenerated lock or edited project metadata.

Cargo performs dependency and feature resolution using the committed manifest,
lockfile and pinned dependencies. CI retains the actual `cargo metadata --locked
--features extension --format-version 1` output as a diagnostic artifact; it does
not maintain a second expected dependency graph or compare feature unions across
platforms. There is no committed dependency-graph JSON or graph-contract generator
or validator. Each receipt retains a positive attempt number; successful
prior-attempt cells remain valid during unchanged-head failed-job reruns because
the original source/run is bound through the build receipt.

Installed environment evidence must name the matching standard CPython minor,
both installed distributions at the source release version, and the base
dependencies satisfying the source's numeric `>=` bounds. Empty, duplicate or
contradictory evidence fails. This captures additional installed dependency
versions without pretending those versions are locked by the native Cargo lock.

The `record` command hashes native and base wheels before installing them and
rejects changes during testing. Aggregation additionally requires `--base DIRECTORY`
and hashes the retained base wheel, rather than merely comparing receipts with
each other. It rehashes every retained native wheel and binds the successful
installation evidence to its original source/run and digest.

After aggregation succeeds, trusted builds attest the original native and base
wheels, receipts, core proof, diagnostic metadata and `native-summary.json`
together. The summary records the successful matrix, source commit, original run
and wheel hashes. The final job uses `gh attestation verify` with the action's
returned bundle, expected repository, signer workflow, workflow source SHA/ref
and hosted-runner requirement. The completion artifact retains both the summary
and `native-attestation.sigstore.json` for later verification.

Release consumers must download the same wheel artifacts and verify the bundle
again before publication, checking the trusted workflow/repository/source and the
intended build commit/run in the signed summary. Prepared source bundles can have
a build commit different from the workflow's commit; authenticate both rather
than treating one as the other. Never rebuild or repackage the tested wheel for
publication. A changed wheel requires fresh testing and signing. A build signature
alone is not test evidence: require the signed successful summary and original
core/install evidence. This interface stage remains `release_qualified: false`
until the later native-request, extras and integration requirements are met.

Run credential-free utility checks with:

```sh
python -m unittest discover -s scripts/native -p 'test_*.py' -v
```

The tests exercise the complete Cartesian product, each missing cell, duplicate
receipts, every unsuccessful job state, altered artifacts/provenance, incompatible
tags, metadata inconsistencies, wrong source/run/base, bundle identity and actual
subprocess PATH/import isolation. They also reject missing core prerequisites,
skipped or altered test reports, absent startup cases and mismatched core wheel,
source/run or vendor proof. Synthetic test wheels are never build evidence.

## Source-bound Linux startup context

The core job uses `native/tests/startup_context.py build` for its actual normal
wheel and passes required `--startup-context` to `core-checks`. Collection passes
that exact context/wheel to the installed native tests, keeps all native cases
(including client lifecycle cases), and rejects every skip/error/failure. Core
proof now retains `startup-context.json`, its SHA256 in `core.json`, and verifies
its source-file/wheel binding during aggregation. Context is covered by the
existing `core-evidence/*` artifact/attestation boundary. All 20 matrix cells,
Rust-free install checks, source/run identity and final aggregate remain required.
See `native/tests/STARTUP.md` for the independent signal ownership boundary,
negative controls, controlled manifest-anchor update and qualification limits.
