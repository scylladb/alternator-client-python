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
checkout production path. This lets later operation/facade cases join the 53
required baseline probes without collection errors or skipped async cases. This
developer check retains Rust and requires `cc`, `cargo`, `rustc`, and Linux `/proc`
before it can start. The C compiler builds a child-local thread-creation fault
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
`core.json`, the original core wheel and the complete `native.xml` report, bound
to the source SHA, original run, wheel hash and vendor provenance hash. This is
separate from all 20 package cells, which still install with Rust absent on PATH.

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
