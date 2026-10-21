# Repository release signing

The public certificate in `publisher.asc` is safe to commit and is registered on
`scylladb-publisher`. Keep it available to consumers; it does not belong in secrets.

* Certification fingerprint: `2CCB493E76F3979F5968A0151E253E2DAAEDB096`
* Signing fingerprint: `445DBF7AC99EDE94850D1A621C6365BBFEB16995`
* Identity: `ScyllaDB Publisher (scylladb/alternator-client-python) <publish.code@scylladb.com>`
* Expiration: `2028-10-19T15:45:34+00:00`

Repository Actions secrets `GPG_PRIVATE_KEY` (ASCII-armored protected signing-subkey
export) and `GPG_PASSPHRASE` (single-line passphrase) supply release signing.
Never upload the full primary private-key backup to CI. Packages, release commits,
and annotated release tags use the same signing subkey, selected by its complete
fingerprint with `!`. The primary key certifies the signing subkey and remains offline.

The `release-version`, `release-prepare`, `release-commit`, `release-build`,
`release-sign`, and `release-tag` Make targets automatically
require the GitHub user `scylladb-publisher` before importing credentials or changing
the checkout.
Locally, the check uses the authenticated `gh` account; switch accounts with
`gh auth switch` if needed. In Actions it checks the triggering user, including
on reruns. Dry runs have the same requirement.

Each signing stage runs in a scoped `signing.py exec` session. The session imports
only the protected signing subkey, checks that primary private material is absent,
and verifies a fresh-agent signing preflight against the pinned public certificate.
Anonymous pipes supply the key import and passphrase; neither credential is written
to a temporary file or included in command-line arguments. Git configuration applies
only to the command and its children. Temporary keyrings are removed on success or
failure, without changing checkout configuration or writing state to `GITHUB_ENV`.

For maintainer-driven release preparation:

```sh
export RELEASE_GPG_PRIVATE_KEY_FILE=/private/path/signing-private.asc
export RELEASE_GPG_PASSPHRASE_FILE=/private/path/passphrase.txt
export RELEASE_VERSION=X.Y.Z
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

Require one `VALIDSIG` record with the signing fingerprint above and the expected
primary fingerprint. `git verify-tag --raw TAG` and `git verify-commit --raw COMMIT`
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
