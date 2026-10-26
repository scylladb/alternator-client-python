# Release notes

The release workflow creates a GitHub release after signed-tag creation and PyPI
publication. Generated notes exclude only PRs carrying `omit-from-release-notes`;
category labels and author identity do not exclude PRs. Installation and
signature-verification guidance stays in the release body.

The Label PRs workflow retains `actions/labeler` for category and omission labels.
The omission rule in `.github/labeler.yml` requires every changed path to match
one support-file union. It permits documentation, tests/fixtures (including
`native/tests`), selected local Jira workflows and explicit development files.
Runtime sources, dependency and packaging manifests, lock files, licensing,
unknown paths and mixed support/runtime changes remain visible. Artifact-building,
signing and publication workflows remain visible, as do release-note control
configuration and the CI caller of native builds. There is no blanket
`.github/**` omission rule.

The guard reads that same omission rule and checks paginated file metadata against
a positive expected count, unique filenames and unchanged PR head/base/count.
Both paths of a rename must qualify. A production-origin rename skips the stock
action and removes the omission marker. After classification, a second snapshot
and the actual logical label are checked; label identity is whole-name and
case-insensitive. Failed or unexpectedly skipped checks attempt marker removal,
preserve unrelated labels and report failure. Removal can fail during API outages
or permission failures. Before/after observations are not atomic and cannot
prevent changes after the last check.

Labeling runs on open, push, reopen, edit and close events, and supports manual
backfills for merged PRs. `sync-labels: true` removes labels when rules stop
matching. Per-PR concurrency serializes these runs. A manual run must select the
repository default branch; it uses that event's trusted tooling, not the old PR
head or base. Its scripts first confirm the target is a PR before any label
write. Confirmation survives later checkout, dependency or classification failures
so cleanup can still be attempted. An ordinary issue or unconfirmed target cannot
have labels changed. This trust boundary assumes maintainers use the repository
workflow; it does not defend against an actor authorized to replace that workflow.

## Before a release

Classification and release publication remain asynchronous. There is no automated
ordering barrier. Before starting the release:

1. Identify the previous published release tag and the prepared release commit.
   List commits with `git rev-list PREVIOUS_TAG..PREPARED_COMMIT`. For each commit,
   query associated PRs with
   `gh api --paginate repos/scylladb/alternator-client-python/commits/COMMIT/pulls`.
   Collect merged PR numbers once each and check the release range; direct commits
   without a PR cannot receive the omission label.
2. Backfill each included PR using Actions → Label PRs → Run workflow. Select the
   current default branch and enter its PR number, or run
   `gh workflow run labeler.yml --ref main -f pull_request=123`.
   If the default branch changes, use that branch instead of `main`.
3. Wait for every requested run to finish successfully. Inspect its final
   `Verified head=... base=... count=... snapshot=... omit=...; actual label checked`
   log and compare the PR's current file list and rename origins with the omission
   policy. A queued, skipped, failed or cancelled run does not establish completion.
   On a failure, check marker cleanup and retry after correcting the cause.
4. Confirm the actual `omit-from-release-notes` label agrees with the verified
   result for every included PR. Re-run classification if the PR revisions or
   policy changed. Preview/review generated notes against the included PR list
   before publication.

Missing this manual verification can leave omissions or unwanted support entries
in published notes. Already published notes and assets remain unchanged by reruns;
no automated synchronization follow-up is scheduled.

After fetching the published release tag, release creation uses
`scripts/release/signing.py verify-tag` with the prepared commit. It requires a
signed annotated tag from the pinned publisher at that commit and uses only the
committed public certificate. It can stop GitHub release creation after PyPI
publication; use the existing partial-release recovery process. It does not prevent
a remote ref changing after verification. Draft releases can be resumed; published
releases are left intact.

The omission marker is GitHub release-note metadata. The local Jira caller ignores
its add/remove label events, including equivalent casing, while forwarding other
events. Existing Jira copies are not removed, and other synchronization paths can
restore copied metadata. Default-`GITHUB_TOKEN` label writes do not trigger another
Actions workflow; this filter also covers manual label events.

Install locked automation test dependencies with
`npm ci --ignore-scripts --prefix .github`, then run
`node --test .github/tests/*.test.cjs`. Run `make test-release-signing` for real
Git/GPG signing and fetched-tag coverage with disposable keys.
