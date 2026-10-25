# Release notes

The existing release workflow creates a GitHub release after the signed tag and
successful PyPI publication. Generated notes exclude only PRs carrying
`omit-from-release-notes`; installation and signature-verification guidance stays
in the release body.

The Label PRs workflow uses `actions/labeler` and `.github/labeler.yml` to apply
that label when all reported changed filenames match documentation,
tests/fixtures (including `native/tests`), CI configuration, or explicitly listed
development-only files. Its single brace-union pattern lets support categories
mix while requiring every filename to match. Production code, unknown paths,
runtime dependency/packaging manifests (`pyproject.toml`, `native/Cargo.toml`,
lock files), licensing and mixed support/runtime PRs stay visible. Existing
category labels and author identity do not exclude PRs.

Labeling runs on open, push, reopen, edit and close events. `sync-labels: true`
removes the omission label when filename rules stop matching. The action reads
trusted repository configuration without checking out PR code. Per-PR concurrency
serializes runs. Matching follows the filenames reported by the stock action;
manual omission labels may be reset by its next run.

Before a release, backfill merged PRs through Actions → Label PRs → Run workflow,
entering each PR number (or use
`gh workflow run labeler.yml -f pull_request=123`). Wait for labeling to finish
before generating release notes. Existing manually added labels are also honored
by GitHub's note generator.

Run `node --test .github/tests/release-workflow.test.cjs` to check the release
publication recovery and signed-tag target verification steps.
