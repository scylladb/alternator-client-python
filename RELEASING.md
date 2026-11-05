# Release Process

This document describes how to release the Alternator Load Balancing Client for
Python.

## Prerequisites

1. You must have write access to the repository and permission to run the
   release workflow.
2. PyPI trusted publishing must authorize this repository's release workflow.
   TestPyPI trusted publishing must authorize `release-dry-run.yml`; PyPI must
   authorize `release.yml`. Both registrations must use owner `scylladb`, repository
   `alternator-client-python`, project `alternator-client` and environment `release`.
   Configure approval by `nikagra` or `dkropachev`, self-review allowed, administrator
   bypass disabled, and deployments restricted to branch `main`.
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
   applies to future releases; it does not change existing ones. Protect `v*` tags
   against updates and deletion through an active tag ruleset with no bypass actors.
   Tag creation must remain permitted.

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

Run **Actions → Release → Run workflow** from `main` and supply the required
version, for example `2.0.2`. Production's optional **skip-tests** defaults to
false; it skips only project unit/integration tests. Release helper tests, lint,
builds, metadata and publication checks always run. Ordinary push/PR CI remains
independent: each release validates its own selected commit.

The production workflow has two jobs:

1. **validate**, without release credentials or environment approval, checks input
   syntax, unused registry version, tooling, unit/integration tests, release helper
   tests (including disposable Git/GPG keys and real builds), lint and an unsigned
   build. It exports the validated baseline SHA and rechecks remote `main`.
2. **release**, on a fresh runner after approval in environment `release`, checks
   that `main` still identifies the validated baseline. It creates one signed
   release commit updating `alternator/_version.py` and `native/pyproject.toml`
   together, builds/checks the pure Python distributions without GPG credentials,
   signs distributions/checksums, and creates a local signed annotated tag. There
   is no development bump, Cargo version change or native package publication.

Before upload, it verifies history, metadata, signatures, tag target and recovery
hashes, saves recovery artifacts and rechecks remote `main`. Publication proceeds
in this order: **PyPI upload → atomic push of commit to `main` and tag → GitHub
release**. A failed stage stops all later stages. If `main` advances during upload,
the exact baseline lease rejects the push and requires manual reconciliation.

Open issues **and pull requests** labelled `release-blocker` stop production before
validation, before approval, after approval, and before each publication stage.
All pages are checked; API errors stop publication. Discovery of any existing
GitHub release or draft also stops automation for manual recovery. New releases
follow draft creation, attachment of verified assets, remote hash verification,
then publication. Assets are never overwritten. Generated notes retain the
`omit-from-release-notes` policy and add installation and signature instructions;
historical release notes are not copied into every release.

For a rehearsal, run **Actions → Release dry run** (`release-dry-run.yml`) from
`main` with a new version unused on TestPyPI. It always runs all tests and uses the
same preparation, signing, recovery and approval path. It uploads only to TestPyPI
and has read-only repository access. It writes no remote Git refs or GitHub
release and omits production blocker checks. Both protected jobs share a
concurrency group with cancellation disabled; validations run independently.

Both workflows retain exact signed distributions, checksums/signatures, a full
Git bundle containing the prepared commit **and annotated tag**, and a manifest
recording baseline, prepared objects, version, registry, run identity and SHA-256
hashes for **30 days**. Publication outcomes/draft receipts and partial preparation
are retained separately for 30 days. Sanitized command logs are retained for
**seven days**. Download recovery artifacts before expiry. The prepublication
recovery upload must succeed before registry upload can start.

Release jobs reject reruns. **Do not use Re-run failed jobs after publication or
an uncertain upload.** Follow stage-specific recovery below, using the original
objects and assets. Never rebuild after uncertain publication.

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

### Publishing Fails: Recovery by Completed Stage

First inspect the original run's steps, `release-stages.json`, registry files,
remote refs and GitHub releases/drafts. An action failure can mean a remote write
succeeded before its response was lost. Do not infer absence from a failed API
call. Repository discovery errors stop recovery until access works again.

Download the original attempt's recovery artifacts into an empty directory:

```bash
gh run download RUN_ID --repo scylladb/alternator-client-python \
  --name release-recovery-1 --dir recovery
gh run download RUN_ID --repo scylladb/alternator-client-python \
  --name release-receipts-1 --dir receipts
git clone recovery/release-source.bundle recovered
cp recovery/release-source.bundle recovery/release-manifest.json recovered/
cp -R recovery/dist recovered/dist
cd recovered
mkdir packages
cp dist/*.whl dist/*.tar.gz packages/
export GITHUB_REPOSITORY=scylladb/alternator-client-python
export RELEASE_VERSION=$(jq -r .version release-manifest.json)
export RELEASE_BASE_COMMIT=$(jq -r .baseline release-manifest.json)
export RELEASE_REGISTRY=$(jq -r .registry release-manifest.json)
python3 scripts/release/pipeline.py verify
```

Use the original attempt number for the artifact names. Verify against the pinned
public certificate at the reviewed baseline. Treat manifests as run evidence,
not as substitutes for signature verification. `verify` requires the exact
baseline, one direct signed child with only the expected metadata edits, original
signed tag object, distribution metadata/signatures/checksums and all recorded
hashes. Recovery never needs the signing private key.

| Last completed stage | Recovery |
| --- | --- |
| Validation or preparation only, definitely no upload | Correct the cause. A fresh dispatch is allowed after confirming the version, tag and GitHub release are unused. If using the saved preparation, preserve its original verified objects/assets. |
| Registry upload failed or outcome uncertain | Compare the registry's file names and downloaded SHA-256 hashes with the manifest. If no files exist, a package owner may upload the original `packages/` files using approved registry credentials. If only some files exist, upload only the missing original files after verifying every existing hash. Never use `skip-existing` or rebuild. |
| Registry complete, SCM push absent | Set `origin` to the canonical repository and authenticate. Recheck blockers and baseline, then use `python3 scripts/release/pipeline.py push` to atomically push both original refs with the exact baseline lease. |
| SCM push uncertain | Read both remote refs. Require the manifest's exact commit and signed tag object. If both match, proceed to GitHub. If either differs or only one exists, stop for manual reconciliation. Never force-push or replace/delete a tag. |
| SCM complete, no GitHub release/draft | After verifying remote refs and registry hashes, run `python3 scripts/release/pipeline.py github`. It rechecks blockers and creates a new draft, attaches verified assets, verifies remote hashes, then publishes. |
| Existing GitHub draft | Inspect the recorded draft id, tag target and every asset name/hash. Attach only missing original assets after verifying existing ones. Recheck blockers and signatures before publishing that exact draft. Automation refuses to adopt drafts; do not delete/recreate them or overwrite assets. |
| GitHub published | Verify immutable tag/assets and registry hashes. No republishing is needed. A discrepancy requires a separate corrective release. |

For an existing draft, obtain its exact asset inventory with
`gh api --paginate repos/scylladb/alternator-client-python/releases/DRAFT_ID/assets`.
Download assets and verify their manifest hashes/signatures. Once all match and
blockers are clear, an authorized maintainer can publish that exact draft with
`gh api --method PATCH repos/scylladb/alternator-client-python/releases/DRAFT_ID -F draft=false`.
The regular helper deliberately stops at any existing draft or release.

If `main` advanced during upload, reconcile manually with maintainers while
preserving the uploaded bytes and original signed commit/tag. Do not reset `main`,
replace the prepared objects, rebuild packages or dispatch the same version.
Do not push only the tag as a workaround. A fresh version may be required.

For TestPyPI recovery, stop after confirming the uploaded package hashes and
signatures. Do not run `push` or `github`. A successful rollout rehearsal must
verify downloaded TestPyPI files against the saved hashes/signatures and confirm
that remote `main`, tags and GitHub releases are unchanged by the rehearsal.

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
