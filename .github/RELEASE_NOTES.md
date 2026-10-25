# Release notes

The existing release workflow creates a GitHub release after the signed tag and
successful PyPI publication. Generated notes exclude only PRs carrying
`omit-from-release-notes`; installation and signature-verification guidance stays
in the release body.

The Classify release notes workflow applies that label when every changed path
is documentation, tests/fixtures (including `native/tests`), CI configuration, or
an explicitly listed development-only file. It never runs PR code. Production
code, unknown paths, runtime dependency/packaging manifests (`pyproject.toml`,
`native/Cargo.toml`, lock files), licensing and mixed PRs stay visible. Renames
must have both old and new paths in the support allowlist. Generic category
labels and author identity do not exclude PRs.

Classification runs on open, push, reopen and close, removing a stale omission
label when a PR stops qualifying. API failures or incomplete/changing file lists
remove the label and fail visibly. Automatic classification owns this label;
manual overrides can be reset by its next run. Per-PR concurrency serializes
classification, and each run reads current PR state.

Before a release, backfill merged PRs through Actions → Classify release notes →
Run workflow, entering each PR number (or use
`gh workflow run label-release-notes.yml -f pull_request=123`). Wait for successful
classification before generating release notes. Existing manually added labels
are also honored by GitHub's note generator.

Run `node --test .github/tests/release-notes.test.cjs` to check the actual trusted
workflow script against mocked GitHub API responses and the release publish step.
