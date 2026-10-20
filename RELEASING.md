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
5. Enable **Settings → General → Releases → Enable release immutability** once.
   The workflow uploads every asset to a draft, then publishes it automatically.
   GitHub locks the release tag and assets when it is published. This setting
   applies to future releases; it does not change existing ones.

## Compatibility and Version Decision

Review all public API removals, return-type changes, and changed defaults before
choosing a version. This project follows Semantic Versioning:

- **MAJOR** for incompatible API or behavior changes
- **MINOR** for backward-compatible functionality
- **PATCH** for backward-compatible fixes

For future releases, changed routing or authentication defaults, removal of
deprecated names or credential arguments, and default-enabled node health or
quarantine behavior also require a major version. Document migration guidance
in [docs/RELEASE_NOTES.md](docs/RELEASE_NOTES.md) when a change needs more
explanation than the automatically generated pull-request list.

Pre-release versions use suffixes such as `-alpha.1`, `-beta.1`, and `-rc.1`.
Python spellings such as `2.1.0a1`, `2.1.0b1`, and `2.1.0rc1` are also accepted.
The workflow normalizes the package version to Python's version format; for
example, input `2.1.0-rc.1` produces package version `2.1.0rc1` and Git tag
`v2.1.0-rc.1`. These spellings identify the same PyPI version and cannot be used
for separate releases. Never reuse a published version.

## Run the Release Workflow

Once the prerequisites are configured, a release needs only a workflow dispatch:

1. Ensure the intended changes are merged into `main` and CI has passed for
   that commit. Open issues labeled `release-blocker` prevent publication.
2. Open **Actions → Release → Run workflow**.
3. Select `main`, enter the new version (for example, `2.0.2`), and run it.
   Leave **dry run** disabled for production.

No manual version-file edit, release-notes edit, or release publication is
required. The workflow:

1. Validates version availability, the main-branch CI result, and release blockers.
2. Sets `alternator/_version.py` to the requested package version and runs unit
   tests, integration tests, and linters.
3. Creates and verifies a signed release commit containing only the version-file
   change. This commit is a child of the selected `main` commit and is published
   through its signed release tag; it is not pushed to `main`.
4. Builds the wheel and source archive from that release commit, validates their
   metadata, and signs the artifacts and checksum manifest.
5. Creates and verifies the signed version tag, then publishes the packages to PyPI.
6. Generates GitHub release notes from the changes since the preceding release,
   including pull requests and contributors, and adds installation and
   verification instructions. Historical contents of `docs/RELEASE_NOTES.md`
   are not copied into every GitHub release.
7. Creates a draft GitHub release, uploads all artifacts, signatures, and
   checksums, and publishes the release automatically. With release immutability
   enabled, this final step locks the tag and assets.

The source version on `main` is a development placeholder. The release tag and
both distributions contain the chosen version. Releases are serialized to
prevent overlapping production runs.

For an optional TestPyPI rehearsal, enable **dry run**. It performs version
preparation, checks, builds, and signing, then uploads to TestPyPI. It does not
push a commit or tag, publish to production PyPI, or create a GitHub release.

The verification commands below can also be used to inspect a published release.

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

The following examples verify 2.0.1. Change `VERSION` for a later release.

### Checksums

`SHA256SUMS` contains entries for both the wheel and source archive, so download
both files before running the check:

```bash
VERSION=2.0.1
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
curl --fail --location https://github.com/scylladb-publisher.gpg | gpg --import
gpg --fingerprint 71A6D22711CDB7C2446D21CFBF4BF97A8D4DF1AA
```

For the historical 2.0.0 release, use its original signing key with fingerprint
`A97AF2DE72D4293398AF8274FC93C043D8ADA78E` instead. The publisher-key update does
not change existing release signatures.

Download and verify the signed checksum manifest:

```bash
VERSION=2.0.1
BASE_URL="https://github.com/scylladb/alternator-client-python/releases/download/v${VERSION}"

curl --fail --location --remote-name "${BASE_URL}/SHA256SUMS"
curl --fail --location --remote-name "${BASE_URL}/SHA256SUMS.asc"
gpg --verify SHA256SUMS.asc SHA256SUMS
```

The workflow also signs each distribution. For example:

```bash
VERSION=2.0.1
BASE_URL="https://github.com/scylladb/alternator-client-python/releases/download/v${VERSION}"
SDIST="alternator_client-${VERSION}.tar.gz"

curl --fail --location --remote-name "${BASE_URL}/${SDIST}"
curl --fail --location --remote-name "${BASE_URL}/${SDIST}.asc"
gpg --verify "${SDIST}.asc" "${SDIST}"
```

### Signed Git Tag

```bash
VERSION=2.0.1
git fetch --tags origin
git tag --verify "v${VERSION}"
```

## Troubleshooting

### Validation Fails

- Use `X.Y.Z` or an accepted prerelease suffix such as `X.Y.Z-rc.1` or `X.Y.Zrc1`.
  The workflow sets `alternator/_version.py` automatically.
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
  run or recreate the tag. The saved release commit and distributions are reused;
  an interrupted GitHub release upload resumes using its existing draft.
- If PyPI accepted only some files before a failure, inspect that release before
  retrying: production uploads reject existing filenames rather than replace
  them. Do not delete published files or move the signed tag.

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
`2.0.2`), and follow the complete release process again. Never try to overwrite
or re-upload `2.0.1`.
