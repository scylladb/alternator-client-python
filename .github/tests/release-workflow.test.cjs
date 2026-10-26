// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync, mkdtempSync, writeFileSync, chmodSync, mkdirSync, rmSync} = require('node:fs');
const {tmpdir} = require('node:os');
const {join, resolve} = require('node:path');
const {spawnSync} = require('node:child_process');
const {test} = require('node:test');
const yaml = require('js-yaml');

const root = resolve(__dirname, '../..');
const release = yaml.load(readFileSync(join(root, '.github/workflows/release.yml'), 'utf8'));
const steps = release.jobs['create-release'].steps;

test('release recovery preserves published releases and resumes drafts or creates absent releases', () => {
  const publish = steps.find(step => step.name === 'Create draft, attach artifacts, and publish release').run;
  for (const state of ['published', 'draft', 'absent']) {
    const dir = mkdtempSync(join(tmpdir(), 'release-notes-'));
    try {
      mkdirSync(join(dir, 'dist'));
      writeFileSync(join(dir, 'dist/package.whl'), 'fixture');
      writeFileSync(join(dir, 'release-body.md'), 'notes');
      // gh release view supports published-by-tag and separate draft lookup.
      // This is its CLI contract, not a draft response from REST's by-tag endpoint.
      writeFileSync(join(dir, 'gh'), '#!/bin/bash\n' +
        'printf "%s\\n" "$*" >> "$CALLS"\n' +
        'if [[ "$1 $2" == "release view" ]]; then\n' +
        '  [[ "$STATE" != absent ]] || exit 1\n' +
        '  [[ "$STATE" == draft ]] && draft=true || draft=false\n' +
        '  printf \'{"isDraft":%s,"url":"https://example.test/release"}\\n\' "$draft"\n' +
        'fi\n');
      chmodSync(join(dir, 'gh'), 0o755);
      const result = spawnSync('bash', ['-c', publish], {cwd: dir, encoding: 'utf8', env: {
        ...process.env, PATH: `${dir}:${process.env.PATH}`, RELEASE_VERSION: '2.0.2',
        PRERELEASE: 'false', STATE: state, CALLS: join(dir, 'calls')
      }});
      assert.equal(result.status, 0, result.stderr);
      const calls = readFileSync(join(dir, 'calls'), 'utf8').trim().split('\n');
      assert.equal(calls[0], 'release view v2.0.2 --json isDraft,url');
      if (state === 'published') {
        assert.equal(calls.length, 1);
        assert.match(result.stdout, /already published/);
      } else {
        assert.match(calls[1], state === 'draft' ? /^release edit .*--notes-file/ : /^release create .*--verify-tag --draft/);
        assert.match(calls[2], /^release upload v2.0.2 dist\/package.whl --clobber$/);
        assert.equal(calls[3], 'release edit v2.0.2 --draft=false');
      }
    } finally { rmSync(dir, {recursive: true}); }
  }
});

test('fetched tag is authenticated with prepared commit before notes and failures stop the step', () => {
  const index = steps.findIndex(step => step.name === 'Verify published tag identifies the signed release commit');
  assert.ok(index < steps.findIndex(step => step.name === 'Build release body'));
  const verify = steps[index].run;
  const dir = mkdtempSync(join(tmpdir(), 'release-tag-'));
  try {
    writeFileSync(join(dir, 'git'), '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALLS"\ntouch "$FETCHED"\n');
    writeFileSync(join(dir, 'python3'), '#!/bin/sh\n' +
      'test -f "$FETCHED" || exit 9\nprintf "%s\\n" "$*" >> "$CALLS"\nexit "$VERIFY_STATUS"\n');
    for (const command of ['git', 'python3']) chmodSync(join(dir, command), 0o755);
    for (const status of [0, 1]) {
      const calls = join(dir, `calls-${status}`);
      const result = spawnSync('bash', ['-c', verify], {encoding: 'utf8', env: {
        ...process.env, PATH: `${dir}:${process.env.PATH}`, RELEASE_VERSION: '2.0.2',
        RELEASE_COMMIT: 'prepared-commit', VERIFY_STATUS: String(status),
        CALLS: calls, FETCHED: join(dir, `fetched-${status}`)
      }});
      assert.equal(result.status, status, result.stderr);
      assert.deepEqual(readFileSync(calls, 'utf8').trim().split('\n'), [
        'fetch --force origin refs/tags/v2.0.2:refs/tags/v2.0.2',
        'scripts/release/signing.py verify-tag v2.0.2 prepared-commit'
      ]);
    }
  } finally { rmSync(dir, {recursive: true}); }
});

test('Jira caller filters only exact logical omission-marker add/remove events', () => {
  const jira = yaml.load(readFileSync(join(root, '.github/workflows/call_jira_sync.yml'), 'utf8'));
  const job = jira.jobs['jira-sync'];
  assert.equal(job.if.replace(/\s+/g, ' ').trim(),
    "${{ !((github.event.action == 'labeled' || github.event.action == 'unlabeled') && github.event.label.name == 'omit-from-release-notes') }}");
  // GitHub expression == is case-insensitive. Normalize strings only in this
  // surrogate so mixed-case logical names have the runtime's equality semantics.
  const expression = job.if.replace(/^\$\{\{\s*|\s*\}\}$/g, '');
  const evaluate = new Function('github', `return ${expression}`);
  for (const action of jira.on.pull_request_target.types) {
    for (const name of ['omit-from-release-notes', 'OMIT-FROM-RELEASE-NOTES',
      'omit-from-release-notes-extra', 'prefix-omit-from-release-notes', 'bug', '']) {
      const skip = ['labeled', 'unlabeled'].includes(action) && name.toLowerCase() === 'omit-from-release-notes';
      assert.equal(evaluate({event: {action, label: {name: name.toLowerCase()}}}), !skip);
    }
  }
  assert.equal(job.uses, 'scylladb/github-automation/.github/workflows/main_pr_events_jira_sync.yml@main');
  assert.equal(job.with.caller_action, '${{ github.event.action }}');
  assert.equal(job.secrets.caller_jira_auth, '${{ secrets.USER_AND_KEY_FOR_JIRA_AUTOMATION }}');
});

test('release-note configuration excludes only the omission marker', () => {
  const config = yaml.load(readFileSync(join(root, '.github/release.yml'), 'utf8'));
  assert.deepEqual(config.changelog.exclude, {labels: ['omit-from-release-notes']});
  assert.ok(config.changelog.categories.some(category => category.labels.includes('*')));
});
