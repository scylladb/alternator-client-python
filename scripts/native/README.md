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
- `attest` defaults to false for pull requests. A trusted caller can enable
  GitHub build attestations for each wheel and its JSON/checksum evidence. It
  also attests the core qualification wheel, `core.json` and `native.xml` so
  release callers can authenticate core proof against its source/original run.
  The caller must grant `contents: read`, `id-token: write` and `attestations: write` to the
  workflow call. There are no signing secrets, publishing credentials or
  protected signing environments in any native build/install job.

The `${artifact-prefix}-core` artifact and required aggregate argument
`--core-evidence DIRECTORY` are part of the P10/P11 integration seam. Those
callers must retain the proof and authenticate its trusted-build attestation
before accepting it. The host-tagged core wheel is qualification-only and must
remain separate from published packages: the release inventory still contains
exactly 20 native wheels, one pure base wheel and one base sdist. It is not a
twenty-first native release wheel.

## Evidence and qualification stages

Each cell artifact contains its original wheel, `SHA256SUMS`, `build.json`,
`install.json`, and `provenance.json`. Build receipts use the coordinated release
shape: `filename`, `sha256`, `commit`, and original Actions `run_id` (all strings).
Provenance also records the Rust compiler, maturin, Rust target, complete enabled
Cargo dependency metadata/revisions, lock and project metadata hashes, base wheel
hash, installed Python/dependency versions, workflow identity, run attempt, runner
architecture and OS. Mac deployment
targets are preserved in the wheel filename and WHEEL metadata.

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

`source_provenance()` reads the verified checkout's native project, Cargo
manifest/lock and toolchain, plus base dependencies, using standard-library TOML.
Aggregation therefore needs Python 3.11+, as pinned in the reusable workflow.
The source/build/install commands still support Python 3.10+. Validation does
not compile code, run Cargo, import installed packages or fetch dependencies.

`validate_provenance()` requires exactly the version-1 fields emitted by
`record()`. Schema, interface stage, build receipt, slot and version must match
the installation and pinned source; target/runner must match that cell. Compiler
release/host and maturin must match source pins. The lock and project hashes
must match exact source-derived bytes: LF on Linux/macOS, or LF/checkout-CRLF on
Windows. Other hashes are rejected. Windows newline conversion is not permission
to accept a regenerated lock or edited project metadata.

Cargo metadata must contain every locked package identity (name/version/source),
the complete locked dependency edges, one native root and workspace, and all
enabled maturin/native/direct dependency features. Collect full
`cargo metadata --locked --features extension --format-version 1`, without
`--no-deps` or platform filtering. Opaque Cargo IDs join references only within
their own document; absolute paths and target directories are never compared
across OSes. Portable enabled features and workflow/repository identity must
agree across cells. Each receipt retains a positive attempt number; successful
prior-attempt cells remain valid during unchanged-head failed-job reruns because
the original source/run is bound through the build receipt.

Installed environment evidence must name the matching standard CPython minor,
both installed distributions at the source release version, and the base
dependencies satisfying the source's numeric `>=` bounds. Empty, duplicate or
contradictory evidence fails. This captures additional installed dependency
versions without pretending those versions are locked by the native Cargo lock.

Later installation/release adapters must keep `source_provenance()`,
`validate_provenance()` and the aggregate cross-cell checks authoritative when
extending `record()` or the installation schema. A new stage/schema or field
requires an explicit validator update and complete negative/positive evidence;
do not bypass validation or discard contradictory fields. If future Cargo lock
files include inactive optional graph branches, derive the expected enabled
graph explicitly from the verified source rather than silently accepting an
arbitrary subset. New base requirement forms likewise need deliberate support.
Recovery callers must bind original workflow identity using authenticated
attestations, not substitute an unrelated current workflow. The core proof,
all 20 cell checks and cryptographic provenance requirements still apply.

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

## Full SDK source graph profile

P4 adds a reviewed, source-owned `native/cargo-provenance.json` contract for the
full SDK graph. The original strict minimal model remains available only for the
three-dependency P3 foundation without dev/build/target dependency tables. Other
source graphs require the explicit contract; a missing contract never falls back
to receipt-derived subsets or direct-only feature checks.

Generate a reviewable contract change from the verified source, using Cargo
rather than a received metadata file:

```sh
python scripts/native/graph_contract.py --output native/cargo-provenance.json
```

Capture runs `cargo metadata --locked --features extension --format-version 1`
itself, without platform filters or `--no-deps`, and checks the inputs before and
afterwards. Contract schema 1 records exact Cargo manifest/lock LF hashes and the
maturin feature-selection projection, all locked identities, explicit inactive
optional identities, and the complete active graph. Each package records local
feature definitions, its resolved direct-plus-transitive feature union, portable
dependency declarations, exact edges, aliases, kinds and target expressions.
Declaration tuples contain name, version requirement, source, optional flag,
default-feature flag, explicit features, rename, kind, target and registry.
Native normal/dev/build/target tables are checked against source TOML. Opaque
Cargo IDs and absolute source/build paths are never cross-runner identities.

The SDK graph contract records every active/locked package and explicit inactive
branch. Separate actual Cargo fixtures exercise a lock with an inactive optional
package and rejection of metadata that enables that unrequested branch. The
committed normalized graph is an explicit reviewed expectation, not a Python
reimplementation of Cargo's resolver. Source contract changes require fresh capture and review;
recording or aggregation never writes or derives the expected graph from a
receipt. All receipt identities, definitions, declarations, edges and resolved
feature unions must match that source-owned expectation exactly.

A Python release-version-only change does not alter Cargo resolution, so it does
not require graph regeneration: the contract binds selected maturin features,
not the Python project version/readme metadata. P3's raw native pyproject hash,
release version and installation/source/run checks still bind each receipt to
all prepared metadata bytes. Changes to Cargo inputs or maturin feature selection
invalidate the contract. P5 owns recapture after its Cargo feature/dependency
changes. Final merged P0 Git pin/lock/source changes likewise require recapture,
including positive and negative source/identity/feature/edge proof. The current
driver source in both lock and graph is the exact declared Git revision
`549c1b43a281f0b6c0644dfcbcbabb24ddafb7c1`. Restoring that Git identity changed no
package versions or enabled features; it does not establish final merged-pin or
release evidence. The PR now targets Python main after #150 with its
PyO3/pyo3-async-runtimes 0.29.0 bridge. Unsuccessful `GET /localnodes` responses
are rejected, and the held-socket lifecycle test proves the configured discovery
deadline independently of the read timeout. A compatible final merged Rust pin,
maintainer approval and renewed source/lock/graph/startup qualification remain
required before marking the draft ready. Retained local host execution covers
Linux x86_64 CPython 3.13 with a `manylinux_2_38` wheel; qualification of the
required `manylinux_2_28` distribution floor, all 20 matrix cells and live Scylla
behavior requires separate exact-head CI evidence.

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
