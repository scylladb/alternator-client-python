# Release Process

This document describes how to release the Alternator Load Balancing Client for
Python.

## Prerequisites

1. You must have write access to the repository and permission to run the
   release workflow.
2. PyPI trusted publishing must authorize this repository's release workflow.
   TestPyPI trusted publishing must also be configured before using the dry-run
   path.
3. GPG signing is required by the workflow. Configure the
   `GPG_PRIVATE_KEY` Actions secret and, when the key is protected, the
   `GPG_PASSPHRASE` secret.
4. Publish the matching public key and record its full fingerprint so release
   signatures can be verified independently.

## Compatibility and Version Decision

Review all public API removals, return-type changes, and changed defaults before
choosing a version. This project follows Semantic Versioning:

- **MAJOR** for incompatible API or behavior changes
- **MINOR** for backward-compatible functionality
- **PATCH** for backward-compatible fixes

The release prepared by this branch is **2.0.0**. It is a major release because
datacenter and rack scopes no longer broaden routing implicitly, and
`AlternatorConfigBuilder.build()` now returns `Config` rather than
`AlternatorConfig`. The complete migration guidance is in
[docs/RELEASE_NOTES.md](docs/RELEASE_NOTES.md).

For future releases, changed routing or authentication defaults, removal of
deprecated names or credential arguments, and default-enabled node health or
quarantine behavior also require a major version. Record the decision and all
user-visible changes in the release notes before publishing.

Pre-release versions use suffixes such as `-alpha.1`, `-beta.1`, and `-rc.1`.
A version already uploaded to PyPI is immutable and must never be reused.

## Prepare the Release

1. Ensure the intended commit is on `main`, all required CI checks pass for
   that exact commit, and the worktree is clean.
2. Review the dated `2.0.0` section in
   [docs/RELEASE_NOTES.md](docs/RELEASE_NOTES.md). Confirm that it describes all
   user-visible changes and migration steps, and update the date if publication
   moves to another day.
3. Update `alternator/_version.py`:

   ```python
   __version__ = "2.0.0"
   ```

4. Run the local release checks:

   ```bash
   make install
   make verify
   make test-integration
   ```

5. Inspect the wheel and source archive produced in `dist/`. Confirm their
   metadata reports version `2.0.0` and that `twine check` passed.
6. Commit the version and documentation changes, open a pull request, and merge
   it only after review and CI complete.

## Run the Release Workflow

1. Open the repository's **Actions** tab.
2. Select the **Release** workflow and click **Run workflow**.
3. Select `main` and enter `2.0.0`. The input must exactly match
   `alternator/_version.py`.
4. Use the dry-run option first when validating TestPyPI publishing. A dry run
   must not create the production tag or GitHub release.
5. For the production run, leave dry run disabled and confirm the selected
   commit is the reviewed `main` commit.

The production workflow validates the version and existing CI result, runs its
release checks, builds the wheel and source archive, signs both artifacts and
their checksum manifest, creates and verifies the signed `v2.0.0` tag, publishes
the packages to PyPI, and attaches the artifacts to the GitHub release.

After the workflow succeeds, verify the PyPI page, GitHub release, checksum
manifest, GPG signatures, and signed tag using the steps below. Also install the
published wheel in a clean environment and perform a basic import/client smoke
test.

## GPG Signing Setup

### Generate a Key

Generate a dedicated release-signing key if the project does not already have
one:

```bash
gpg --full-generate-key
gpg --list-secret-keys --keyid-format LONG
```

Use a strong key, protect the private key appropriately, and retain a secure
revocation certificate and backup. Record and review the full fingerprint, not
only a short key ID.

### Export the Private Key for GitHub Actions

```bash
gpg --armor --export-secret-keys FULL_FINGERPRINT > private-key.asc
```

Add these repository Actions secrets under **Settings → Secrets and variables
→ Actions**:

| Secret | Required | Description |
| --- | --- | --- |
| `GPG_PRIVATE_KEY` | Yes | Complete ASCII-armored private key, including the begin/end markers |
| `GPG_PASSPHRASE` | If protected | Passphrase for the exported private key |

Securely delete the exported private-key file after configuring the secret.

### Publish the Public Key

```bash
gpg --armor --export FULL_FINGERPRINT > public-key.asc
gpg --keyserver keyserver.ubuntu.com --send-keys FULL_FINGERPRINT
gpg --fingerprint FULL_FINGERPRINT
```

Publish the public key through another project-controlled channel as well, and
ensure the documented fingerprint matches the workflow's signing key.

## Verify a Release

The following examples verify 2.0.0. Change `VERSION` for a later release.

### Checksums

`SHA256SUMS` contains entries for both the wheel and source archive, so download
both files before running the check:

```bash
VERSION=2.0.0
BASE_URL="https://github.com/scylladb/alternator-client-python/releases/download/v${VERSION}"

curl --fail --location --remote-name "${BASE_URL}/SHA256SUMS"
curl --fail --location --remote-name \
  "${BASE_URL}/alternator_client-${VERSION}-py3-none-any.whl"
curl --fail --location --remote-name \
  "${BASE_URL}/alternator_client-${VERSION}.tar.gz"

sha256sum --check --strict SHA256SUMS
```

Run this in an otherwise empty directory so a stale file cannot be mistaken for
the artifact being checked. A successful verification reports `OK` for both
distribution files.

### GPG Signatures

Import the published key and compare the displayed fingerprint with the one
published through the project's trusted channel:

```bash
gpg --keyserver keyserver.ubuntu.com \
  --recv-keys A97AF2DE72D4293398AF8274FC93C043D8ADA78E
gpg --fingerprint A97AF2DE72D4293398AF8274FC93C043D8ADA78E
```

Download and verify the signed checksum manifest:

```bash
VERSION=2.0.0
BASE_URL="https://github.com/scylladb/alternator-client-python/releases/download/v${VERSION}"

curl --fail --location --remote-name "${BASE_URL}/SHA256SUMS"
curl --fail --location --remote-name "${BASE_URL}/SHA256SUMS.asc"
gpg --verify SHA256SUMS.asc SHA256SUMS
```

The workflow also signs each distribution. For example:

```bash
VERSION=2.0.0
BASE_URL="https://github.com/scylladb/alternator-client-python/releases/download/v${VERSION}"
SDIST="alternator_client-${VERSION}.tar.gz"

curl --fail --location --remote-name "${BASE_URL}/${SDIST}"
curl --fail --location --remote-name "${BASE_URL}/${SDIST}.asc"
gpg --verify "${SDIST}.asc" "${SDIST}"
```

### Signed Git Tag

```bash
VERSION=2.0.0
git fetch --tags origin
git tag --verify "v${VERSION}"
```

## Troubleshooting

### Validation Fails

- Ensure the workflow input exactly matches `alternator/_version.py`.
- Use `X.Y.Z` or an accepted prerelease suffix such as `X.Y.Z-rc.1`.
- Ensure `vX.Y.Z` does not already exist and the version has never been
  published to the target package index.
- Confirm the workflow is running for the reviewed commit on `main`.

### GPG Signing Fails

- Ensure `GPG_PRIVATE_KEY` contains the complete ASCII-armored private key.
- Ensure `GPG_PASSPHRASE` matches the key when it is passphrase-protected.
- Check that the signing key is not expired or revoked.
- Confirm the imported key has signing capability.

### Publishing Fails

- Confirm the PyPI or TestPyPI trusted publisher matches the repository,
  workflow filename, environment, and owner configured in GitHub.
- Confirm the target version does not already exist. PyPI does not allow a file
  or version to be replaced by rerunning the workflow.
- If the signed tag succeeded but publishing or GitHub release creation failed,
  use **Re-run failed jobs** on that same workflow run. Do not dispatch a new
  run or recreate the tag.

## Yank a Bad Release

Yanking is not a Twine operation. A project owner must use the PyPI
release-management page:

1. Open <https://pypi.org/manage/project/alternator-client/releases/>.
2. Choose **Options** beside the affected release, then **Yank**.
3. Supply a clear reason so package installers and users can report it.

Yanking is preferable to deleting: it excludes the release from normal version
selection while preserving installations that pin that exact version. See the
[PyPI yanking documentation](https://docs.pypi.org/project-management/yanking/)
for the precise behavior.

Keep the signed GitHub release and tag as an audit trail, and mark the release
notes as yanked. Fix the issue on `main`, choose a new version (for example,
`2.0.1`), and follow the complete release process again. Never try to overwrite
or re-upload `2.0.0`.
