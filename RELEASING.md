# Release Process

This document describes how to release a new version of the Alternator Load Balancing Client for Python.

## Prerequisites

1. You must have write access to the repository
2. The `pypi` environment must be configured in GitHub with PyPI trusted publishing
3. (Optional) GPG signing keys must be configured for signed releases

## Creating a Release

### 1. Update Version

Update the version in `alternator/_version.py`:

```python
__version__ = "1.0.0"
```

Commit and push this change to the main branch.

### 2. Trigger the Release Workflow

1. Go to the repository's **Actions** tab
2. Select the **Release** workflow
3. Click **Run workflow**
4. Enter the version number (must match `_version.py`)
5. Click **Run workflow**

The workflow will:
- Validate the version format and match with `_version.py`
- Run all tests and linters
- Build the package (wheel and source distribution)
- Sign artifacts with GPG
- Generate SHA256 checksums
- Publish to PyPI
- Create a GitHub release with the tag `vX.Y.Z`

## GPG Signing Setup

GPG signing provides cryptographic verification that releases are authentic and haven't been tampered with.

### Generate a GPG Key (if needed)

```bash
# Generate a new key
gpg --full-generate-key

# Choose:
# - RSA and RSA
# - 4096 bits
# - Key does not expire (or set expiration)
# - Your name and email

# List your keys
gpg --list-secret-keys --keyid-format LONG

# Example output:
# sec   rsa4096/ABCD1234EFGH5678 2024-01-01 [SC]
#       1234567890ABCDEF1234567890ABCDEF12345678
# uid                 [ultimate] Your Name <your@email.com>
# ssb   rsa4096/IJKL9012MNOP3456 2024-01-01 [E]
```

### Export the Key for GitHub

```bash
# Export the private key (ASCII armored)
gpg --armor --export-secret-keys ABCD1234EFGH5678 > private-key.asc

# The output will look like:
# -----BEGIN PGP PRIVATE KEY BLOCK-----
# ...
# -----END PGP PRIVATE KEY BLOCK-----
```

### Configure GitHub Secrets

Add the following secrets to your repository (Settings → Secrets and variables → Actions):

| Secret Name | Description |
|-------------|-------------|
| `GPG_PRIVATE_KEY` | The full ASCII-armored private key (including BEGIN/END markers) |
| `GPG_PASSPHRASE` | The passphrase for the GPG key (leave empty if no passphrase) |

### Publish Your Public Key

For users to verify signatures, they need your public key:

```bash
# Export public key
gpg --armor --export ABCD1234EFGH5678 > public-key.asc

# Upload to a keyserver
gpg --keyserver keyserver.ubuntu.com --send-keys ABCD1234EFGH5678
```

Add the key ID and fingerprint to your project documentation so users know how to verify.

## Verifying a Release

### Checksum Verification

```bash
# Download the checksums file
curl -LO https://github.com/scylladb/alternator-client-python/releases/download/v1.0.0/SHA256SUMS

# Download the package
curl -LO https://github.com/scylladb/alternator-client-python/releases/download/v1.0.0/alternator_client-1.0.0.tar.gz

# Verify
sha256sum -c SHA256SUMS
```

### GPG Signature Verification

```bash
# Import the signing key (first time only)
gpg --keyserver keyserver.ubuntu.com --recv-keys A97AF2DE72D4293398AF8274FC93C043D8ADA78E

# Download the signature
curl -LO https://github.com/scylladb/alternator-client-python/releases/download/v1.0.0/SHA256SUMS.asc

# Verify the checksums file signature
gpg --verify SHA256SUMS.asc SHA256SUMS

# Or verify individual files
curl -LO https://github.com/scylladb/alternator-client-python/releases/download/v1.0.0/alternator_client-1.0.0.tar.gz.asc
gpg --verify alternator_client-1.0.0.tar.gz.asc alternator_client-1.0.0.tar.gz
```

### Verify Git Tag Signature

```bash
# Fetch tags
git fetch --tags

# Verify tag signature
git tag -v v1.0.0
```

## Troubleshooting

### Release workflow fails at validation

- Ensure the version in `_version.py` matches exactly what you entered
- Version format must be `X.Y.Z` or `X.Y.Z-suffix` (e.g., `1.0.0-beta.1`)

### GPG signing fails

- Verify `GPG_PRIVATE_KEY` secret contains the full key including headers
- Verify `GPG_PASSPHRASE` is correct (or empty if no passphrase)
- Check the key hasn't expired

### PyPI publishing fails

- Ensure the `pypi` environment is configured with trusted publishing
- Check that the version doesn't already exist on PyPI

## Rollback / Yanking a Bad Release

If a release needs to be removed:

### Yank from PyPI

```bash
# Yank a specific version (marks it as not recommended but still installable
# by pinned requirements)
pip install twine
twine yank alternator-client==1.0.0
```

### Delete the GitHub Release

1. Go to the repository's **Releases** page
2. Find the release (e.g., `v1.0.0`)
3. Click **Delete** on the release
4. Optionally delete the git tag: `git push --delete origin v1.0.0`

### Re-release with a Fix

1. Fix the issue on the main branch
2. Bump the version to a new patch version (e.g., `1.0.1`)
3. Follow the normal release process above

**Note:** PyPI does not allow re-uploading the same version number. You must always use a new version.

## Version Numbering

This project follows [Semantic Versioning](https://semver.org/):

- **MAJOR** version for incompatible API changes
- **MINOR** version for backwards-compatible functionality additions
- **PATCH** version for backwards-compatible bug fixes

Pre-release versions use suffixes like `-alpha.1`, `-beta.1`, `-rc.1`.
