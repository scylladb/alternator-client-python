# Release Process

This document describes how to release the Alternator Load Balancing Client for
Python.

## Prerequisites

1. You must have write access to the repository and permission to run the
   release workflow.
2. PyPI trusted publishing must authorize this repository's release workflow.
   TestPyPI trusted publishing must also be configured before using the dry-run
   path. Both trusted-publisher registrations must require environment `release`.
   Configure its maintainer approval, self-review prevention, administrator-bypass
   prohibition, and `main`-only branch policy before using this workflow.
3. GPG signing is required by the workflow. Configure `GPG_PRIVATE_KEY` with the
   protected repository signing-subkey export and `GPG_PASSPHRASE` with its
   nonempty single-line passphrase in the protected `release` environment. Both
   environment secrets are required; see
   [GPG Signing Setup](#gpg-signing-setup).
4. Publish the public certificate matching the pinned certification and signing
   fingerprints in [scripts/release/SIGNING.md](scripts/release/SIGNING.md) so
   release signatures can be verified independently.
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
   metadata in a separate step without signing secrets, then signs the artifacts
   and checksum manifest in a new scoped session. The commit signing session has
   already removed its temporary keyring before the build starts.
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
preparation, checks, builds, and signing, then uploads to TestPyPI. Rehearsals
also require `main` and environment approval. It does not
push a commit or tag, publish to production PyPI, or create a GitHub release.

The verification commands below can also be used to inspect a published release.

## GPG Signing Setup

Use the repository-specific protected signing subkey documented in
[scripts/release/SIGNING.md](scripts/release/SIGNING.md). That guide covers the
pinned fingerprints, local Make targets, account requirements, and rotation.
Keep the certification primary private key offline.

Export only the protected signing subkey for protected environment secrets:

```bash
gpg --armor --export-secret-subkeys '445DBF7AC99EDE94850D1A621C6365BBFEB16995!' > signing-private.asc
```

| Secret | Required | Description |
| --- | --- | --- |
| `GPG_PRIVATE_KEY` | Yes | Complete ASCII-armored protected signing-subkey export |
| `GPG_PASSPHRASE` | Yes | Nonempty single-line passphrase protecting that subkey |

Securely delete the exported file after configuring the secret. Never upload the
full primary private-key backup to CI. Local release targets require the authenticated
`gh` account `scylladb-publisher`. Actions operators are authorized by the protected
`release` environment and may differ from the signing identity, including reruns.
Follow the [environment rollout and secret migration](scripts/release/SIGNING.md#protected-environment-rollout)
before using the migrated workflow. Historical release dispatch is unsupported.

## Verify a Release

The following examples verify 2.0.1. Change `VERSION` for a later release and use
that release's signing certificate. Releases using the repository-specific key
have certification fingerprint `2CCB493E76F3979F5968A0151E253E2DAAEDB096` and signing
fingerprint `445DBF7AC99EDE94850D1A621C6365BBFEB16995` (see
[scripts/release/SIGNING.md](scripts/release/SIGNING.md)). The old fingerprints
below apply to historical releases only.

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

- Ensure `GPG_PRIVATE_KEY` contains only the complete ASCII-armored protected
  signing-subkey export, without usable primary private material.
- Ensure `GPG_PASSPHRASE` is nonempty and matches the protected signing subkey.
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
