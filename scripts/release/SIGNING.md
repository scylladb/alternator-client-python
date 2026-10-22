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
environment on **every** signing and publishing job. Approved operators may differ
from `scylladb-publisher`, including when another approved maintainer reruns a job.
The signer remains the pinned publisher key. `GITHUB_ACTIONS` alone is not an
approval credential; the actual environment protections enforce Actions access.

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
steps receive signing secrets. These scopes prevent direct credential inheritance;
they do not provide separate runners or protection against arbitrary same-user
persistence. `release-tag` requires `release-source.bundle` from preparation and
checks that its advertised commit matches both the current commit and the requested
release version before creating or accepting the signed tag. These targets prepare
local outputs; the workflow handles uploads and publication. Do not change or replace
existing tags.
Version validation and prerelease normalization live in the `release-version`
Make target; `VERSION_FILE` defaults to `alternator/_version.py`.

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

Before merging the environment-aware workflow, create **Settings → Environments →
release**. Require one approval from the repository's designated release-maintainer
team, prevent self-review, disable administrator bypass, and permit only the
`main` branch. Confirm the actual team with repository administrators; this code
cannot choose that team or configure approval rules. Every job may require its own
approval. The same branch restriction applies to TestPyPI rehearsals.

Upload `GPG_PRIVATE_KEY` and `GPG_PASSPHRASE` into that environment using the original
secure material. Existing GitHub secrets cannot be downloaded. Pass values through
stdin, such as `gh secret set GPG_PRIVATE_KEY --repo scylladb/alternator-client-python
--env release < /secure/signing-private.asc`; do not use arguments containing the
secret itself. Validate the protected subkey and reject empty/multiline passphrases
before uploading. Update both PyPI and TestPyPI trusted-publisher registrations to
require the `release` environment in `release.yml`.

Run an approved TestPyPI rehearsal from `main`, including a rerun by a different
approved maintainer, and check approvals, pinned signatures, log artifacts, and
keyring cleanup. This rehearsal uploads packages to TestPyPI; it is not upload-free.
Only after that succeeds remove the repository-level signing secret copies. Do not
remove them before old workflows have been migrated, or retain them indefinitely.
No workflow has been dispatched and no live signing secrets are used by the tests.

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
  signed Python release commit after that ancestor. Python has no separate
  development-version commit. Tagging additionally checks the prepared bundle,
  version, signed annotated tag, and exact target. Published tags are never moved.
- Historical release dispatch is unsupported: every workflow run must start on
  `main`. Consumer verification of historical certificates remains supported.
- Build and metadata tooling run after the signing session ends, with credential
  inputs and runtime signing settings removed. The build and metadata frontends
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
