# Repository release signing

The public certificate in `publisher.asc` is safe to commit and is registered on
`scylladb-publisher`. Keep it available to consumers; it does not belong in secrets.

* Certification fingerprint: `2CCB493E76F3979F5968A0151E253E2DAAEDB096`
* Signing fingerprint: `445DBF7AC99EDE94850D1A621C6365BBFEB16995`
* Identity: `ScyllaDB Publisher (scylladb/alternator-client-python) <publish.code@scylladb.com>`
* Expiration: `2028-10-19T15:45:34+00:00`

Protected `release` environment secrets `GPG_PRIVATE_KEY` (ASCII-armored protected signing-subkey
export) and `GPG_PASSPHRASE` (single-line passphrase) supply release signing.
Never upload the full primary private-key backup to CI. Packages, release commits,
and annotated release tags use the same signing subkey, selected by its complete
fingerprint with `!`. The primary key certifies the signing subkey and remains offline.

The release workflow authorizes maintainers through the protected `release`
environment on its single signing and publishing job. Approved operators may differ
from `scylladb-publisher`; the pinned publisher key remains the signing identity.
The signer remains the pinned publisher key. Workflow Make invocations pass
`RELEASE_WORKFLOW=1` as a command-line argument, which skips the local account check
only when `GITHUB_ACTIONS=true`; recursive Make calls inherit that argument.
Ambient `GITHUB_ACTIONS` and `RELEASE_WORKFLOW` variables alone do not skip the
check. This explicit route selects workflow orchestration; it is not an approval
credential or runner attestation. The actual environment protections enforce
Actions access.

Locally, all public and internal release Make targets require the authenticated
`gh` account `scylladb-publisher` before importing credentials or changing the
checkout. Switch with `gh auth switch` when needed. The low-level signing library
has no account restriction and does not grant publication permissions.

Each signing stage runs in a scoped `signing.py exec` session. The session imports
only the protected signing subkey, checks that primary private material is absent,
rechecks protection after first use,
and verifies a fresh-agent signing preflight against the pinned public certificate.
Anonymous pipes supply the key import and passphrase; neither credential is written
to a temporary file or included in command-line arguments. Git configuration applies
only to the command and its children. Temporary keyrings are removed on success,
failure, or catchable cancellation, without changing checkout configuration or writing state to `GITHUB_ENV`.

For maintainer-driven release preparation:

```sh
export RELEASE_GPG_PRIVATE_KEY_FILE=/private/path/signing-private.asc
export RELEASE_GPG_PASSPHRASE_FILE=/private/path/passphrase.txt
export RELEASE_VERSION=X.Y.Z
export RELEASE_BASE_COMMIT=$(git rev-parse HEAD)
make release-prepare
# Run any additional checks against the prepared commit and signed distributions.
make release-tag
```

`release-prepare` sets the version, creates a signed detached release commit and
source bundle, builds and checks distributions, generates checksums, then signs
and verifies all three artifacts in one batch. The commit session ends and removes
its keyring before building. Account and version validation finish before the
commit signing session opens. Package tooling and metadata checks receive neither
release credential values/file paths nor session Git/GPG configuration. Artifact
signing uses a separate session after the build succeeds. In Actions these are
separate `release-commit`, `release-build`, and `release-sign` steps; only the signing
steps receive signing secrets. Validation runs first in a separate job without a
protected environment or signing secrets. Only after validation succeeds does the
release-preparation job request environment approval and start on a fresh runner.
It checks out the exact validated SHA and recreates the requested version; no
validation workspace or executable artifacts are restored. Building and artifact
signing still share this release runner, so credential scoping does not protect
against arbitrary build code or same-user persistence within that runner.
`release-tag` requires `release-source.bundle` from preparation and checks that its advertised commit matches both the current commit and the requested
release version before creating or accepting the signed tag. These targets prepare
local outputs; the workflow handles uploads and publication. Do not change or replace
existing tags.
Version validation and prerelease normalization live in `version.py`, called by
`release-version`. Normal preparation updates `alternator/_version.py` and
`native/pyproject.toml` together; Cargo metadata and native publication stay separate.
`VERSION_FILE` (and optional `NATIVE_VERSION_FILE`) support isolated fixtures.
After tagging, the source bundle includes both the commit and signed tag with
full history. The workflow verifies and retains the bundle, manifest and exact
signed distributions before upload. See [stage-specific recovery](../../RELEASING.md#publishing-fails-recovery-by-completed-stage).

Complex pre-release checks can run before preparation or against the signed outputs
after the session closes. Verification needs only the public certificate. Checks
that need to create additional signatures can run inside their own scoped invocation:

```sh
python3 scripts/release/signing.py exec make YOUR_CHECK_TARGET
```

The low-level signing helper does not impose release-account policy. Wrapped
commands have no overall time limit; each GPG operation has its own timeout and
reads the current passphrase, so long checks do not depend on an agent cache.
A failed command propagates its exit status and still cleans up the session.
Agent-shutdown errors emit a fixed warning without replacing that result. Signal
handlers are installed before credential reads; nested commands and GPG processes
share the root command group so it can stop every descendant before removing the
keyring. Uncatchable kills and machine failures still require
runner cleanup.
Helper failures report a fixed operation label and exit status or error category,
without including command arguments, captured output, or exception payloads.

Consumers should obtain this public certificate from this repository at a reviewed
commit, check the certification fingerprint above through a trusted channel, and
verify release signatures. The Ubuntu keyserver can also serve the certificate:

```sh
gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys 2CCB493E76F3979F5968A0151E253E2DAAEDB096
gpg --fingerprint 2CCB493E76F3979F5968A0151E253E2DAAEDB096
gpg --status-fd 1 --verify ARTIFACT.asc ARTIFACT
```

Require matching `GOODSIG` and `VALIDSIG` records, with exactly one signature, with the signing fingerprint above and the expected
primary fingerprint. Reject expired, revoked, bad, or ambiguous signatures even
when a `VALIDSIG` record is present. `git verify-tag --raw TAG` and `git verify-commit --raw COMMIT`
provide the equivalent check for Git objects. Keyserver presence alone establishes
neither trust nor the expected repository identity.

Rotate by preparing a new independent certificate, validating its protected subkey,
registering its public certificate on the publisher account, publishing the public
certificate, reviewing fingerprint/public-certificate updates together, and updating
the secret pair in a coordinated release pause. Keep old public certificates available
for historical releases. Never revoke the previous key merely to rotate it. If a key
is compromised, stop releases and use the offline revocation certificate; fix the
exposure before resuming. Do not rerun publication after a partial release without
following the repository's recovery instructions.

## Protected environment rollout

Configure **Settings → Environments → release** to require one approval from
`nikagra` or `dkropachev`, allow self-review, disable administrator bypass and
permit only branch `main`. Both workflows use one protected job after independent
validation. Production and rehearsal share job-level concurrency and never cancel
an active publication.

Upload environment `GPG_PRIVATE_KEY` and `GPG_PASSPHRASE` from the original secure
material; GitHub secrets cannot be downloaded. Supply values through stdin, for
example `gh secret set GPG_PRIVATE_KEY --repo scylladb/alternator-client-python
--env release < /secure/signing-private.asc`. Validate the pinned protected subkey
and nonempty single-line passphrase before use. Configure PyPI's publisher for
`release.yml`, TestPyPI's for `release-dry-run.yml`, both in environment `release`.

Enable immutable releases and an active `v*` tag ruleset prohibiting updates and
deletion without bypass actors. Existing published releases remain unchanged.
Run an approved TestPyPI rehearsal from `main`; verify downloaded distribution
hashes/signatures against the retained manifest and confirm there were no remote
Git or GitHub release writes. Only then remove repository-level signing secret
copies. Rerunning release jobs is prohibited: recover the original objects/assets
by completed stage as documented in [RELEASING.md](../../RELEASING.md).

## Reference parity and test coverage

This follows the repository-scoped signing design in the release reference, with
Python-specific release preparation in place of Maven's release/SCM plugins:

- The publisher key, operator authorization, Git writer, and PyPI OIDC publisher
  are separate identities. Actions uses environment approval; local Make uses the
  explicit publisher-account policy.
- Scoped sessions pin both complete fingerprints, reject usable primary secrets
  and unprotected subkeys, preflight with a fresh agent and a public-only keyring,
  and apply inherited runtime Git configuration without persisting checkout state.
- `RELEASE_BASE_COMMIT` must be exported in the calling shell before preparation;
  Make cannot set the caller's environment. Verification requires exactly one new
  signed, single-parent Python release commit directly after that baseline.
  With `RELEASE_VERSION` supplied, verification checks the exact commit subject
  and both expected metadata edits, rejecting unrelated changes. Python has no separate
  development-version commit. Tagging additionally checks the prepared bundle,
  version, signed annotated tag, and exact target. Published tags are never moved.
- Historical release dispatch is unsupported: every workflow run must start on
  `main`. Consumer verification of historical certificates remains supported.
- Build and metadata tooling run after the signing session ends, with credential
  inputs, GitHub/OIDC tokens and runtime signing settings removed. The build and metadata frontends
  are pinned. A fresh session signs the resulting distributions and checksums.
- `guard.py` rejects debug/credential-bearing command arguments, captures at most
  8 MiB across stdout/stderr in memory, and redacts complete streams and encoded
  secret forms before printing or writing `release-logs/`. Partial/overflow output
  is withheld. Workflows upload only these sanitized command logs, even on failure.
  These checks do not sandbox arbitrary build code or same-user persistence.

`make test-release-signing` uses disposable keys, real Make/Git/GPG and a controlled
package-tool substitute. It covers signatures, history, credentials, cleanup,
actor/rerun differences, redaction, overflow, and nested command cancellation.
`RELEASE_REAL_BUILD_TEST=1 make test-release-signing` additionally runs the pinned
real Python build and Twine metadata check on a disposable project, signs its wheel,
source archive and checksum manifest, restores its source bundle into a separate
checkout, and independently verifies the Git history, tag, and artifact signatures.
PR CI runs both levels. No publishing command is run by these tests. The Maven
Release/SCM/GPG-plugin smoke test is inapplicable to this Python repository.

Real environment approval, OIDC publication and migration remain rollout checks;
unit tests with actor variables cannot establish those protections.
