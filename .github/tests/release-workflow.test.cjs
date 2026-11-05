// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync, mkdtempSync, writeFileSync, chmodSync, rmSync} = require('node:fs');
const {tmpdir} = require('node:os');
const {spawnSync} = require('node:child_process');
const {join, resolve} = require('node:path');
const {test} = require('node:test');
const yaml = require('js-yaml');
const root = resolve(__dirname, '../..');
const load = path => yaml.load(readFileSync(join(root, path), 'utf8'));
const production = load('.github/workflows/release.yml');
const dry = load('.github/workflows/release-dry-run.yml');
const validate = load('.github/actions/release-validate/action.yml');
const prepare = load('.github/actions/release-prepare/action.yml');

test('workflows expose required versions, bounded skipping, and one serialized protected job', () => {
  assert.deepEqual(Object.keys(production.on.workflow_dispatch.inputs), ['version', 'skip-tests']);
  assert.equal(production.on.workflow_dispatch.inputs['skip-tests'].default, false);
  assert.deepEqual(Object.keys(dry.on.workflow_dispatch.inputs), ['version']);
  for (const workflow of [production, dry]) {
    assert.equal(workflow.on.workflow_dispatch.inputs.version.required, true);
    assert.equal(workflow.concurrency, undefined);
    assert.deepEqual(Object.keys(workflow.jobs), ['validate', 'release']);
    const {validate: checks, release} = workflow.jobs;
    assert.equal(checks.environment, undefined);
    assert.equal(checks.permissions.contents, 'read');
    assert.equal(checks.permissions['id-token'], undefined);
    assert.ok(!JSON.stringify(checks).includes('secrets.'));
    assert.equal(checks.outputs.baseline, '${{ steps.validate.outputs.baseline }}');
    assert.equal(release.needs, 'validate');
    assert.equal(release.environment, 'release');
    assert.deepEqual(release.concurrency, {group: 'alternator-python-release', 'cancel-in-progress': false});
    assert.equal(release.permissions['id-token'], 'write');
    assert.equal(release.steps[0].with.ref, '${{ needs.validate.outputs.baseline }}');
    assert.ok(!JSON.stringify(release).includes('download-artifact'));
    assert.ok(!JSON.stringify(workflow).includes('skip-existing'));
  }
  const helper = validate.runs.steps.find(step => step.name === 'Test release automation');
  assert.equal(helper.if, undefined);
  assert.ok(helper.run.includes('make test-release-signing'));
  assert.ok(helper.run.includes('node --test'));
  const tests = validate.runs.steps.find(step => step.name === 'Project unit and integration tests');
  assert.equal(tests.if, "inputs.skip-tests != 'true'");
  for (const step of validate.runs.steps.filter(step => step !== tests)) assert.equal(step.if, undefined);
  assert.ok(validate.runs.steps[0].run.includes('test "$GITHUB_REF" = refs/heads/main'));
  assert.ok(prepare.runs.steps[0].run.includes('pipeline.py main'));
});

test('main advancing at validation, approval and final upload gates stops publication', () => {
  const validationGate = validate.runs.steps.find(step => step.name === 'Recheck main after validation');
  const approvalGate = prepare.runs.steps.find(step => step.name === 'Verify validated baseline after approval');
  assert.ok(validationGate);
  assert.ok(approvalGate);
  assert.equal(validate.runs.steps.at(-1), validationGate);
  assert.equal(prepare.runs.steps[0], approvalGate);
  for (const gate of [validationGate, approvalGate]) {
    const dir = mkdtempSync(join(tmpdir(), 'release-main-'));
    try {
      writeFileSync(join(dir, 'git'), '#!/bin/sh\n' +
        'case "$1" in\n' +
        'rev-parse) printf "base\\n";;\n' +
        'ls-remote) printf "%s\\trefs/heads/main\\n" "$REMOTE_MAIN";;\n' +
        '*) exit 90;;\nesac\n');
      chmodSync(join(dir, 'git'), 0o755);
      for (const main of ['base', 'advanced']) {
        const result = spawnSync('bash', ['-e', '-o', 'pipefail', '-c', gate.run], {
          cwd: root, encoding: 'utf8', env: {...process.env, PATH: `${dir}:${process.env.PATH}`,
            GITHUB_REF: 'refs/heads/main', GITHUB_SHA: 'base', GITHUB_RUN_ATTEMPT: '1',
            RELEASE_BASE_COMMIT: 'base', REMOTE_MAIN: main}
        });
        assert.equal(result.status === 0, main === 'base', result.stderr);
        if (main === 'advanced') assert.match(result.stderr, /Remote main advanced/);
      }
    } finally { rmSync(dir, {recursive: true}); }
  }
  // Upload gates must call the helper whose final operation compares remote main.
  for (const workflow of [production, dry]) {
    const steps = workflow.jobs.release.steps;
    const upload = steps.findIndex(step => step.id === 'registry');
    assert.match(steps[upload - 1].run, /pipeline.py checkpoint/);
  }
});

test('release write credentials are confined to publication and blocker steps', () => {
  const release = production.jobs.release;
  assert.equal(release.env.GH_TOKEN, undefined);
  assert.equal(release.steps[0].with['persist-credentials'], false);
  const secretSteps = release.steps.filter(step => step.env?.GH_TOKEN);
  assert.deepEqual(secretSteps.map(step => step.name), ['Recheck blockers after approval',
    'Final checks before registry upload', 'Atomically push release commit to main and signed tag',
    'Publish immutable GitHub release']);
  assert.equal(prepare.runs.steps.find(step => step.name === 'Verify history and build distributions').env, undefined);
});

test('publication preserves ordering, credentials scope and recovery retention', () => {
  const steps = production.jobs.release.steps;
  const index = name => steps.findIndex(step => step.name === name);
  const names = ['Prepare signed release', 'Save verified recovery artifacts before publication',
    'Final checks before registry upload', 'Publish to PyPI',
    'Atomically push release commit to main and signed tag', 'Publish immutable GitHub release'];
  for (let i = 1; i < names.length; i++) assert.ok(index(names[i - 1]) < index(names[i]));
  for (const step of steps.slice(index(names[0]), index(names.at(-1)) + 1)) {
    assert.equal(step.if, undefined);
    assert.equal(step['continue-on-error'], undefined);
  }
  assert.equal(steps[index('Publish to PyPI')].with['packages-dir'], 'packages/');
  for (const step of prepare.runs.steps) {
    if (step.name.includes('build')) {
      assert.equal(step.env, undefined);
      assert.ok(!step.run.includes('inputs.passphrase'));
    }
  }
  for (const workflow of [production, dry]) {
    for (const [job, config] of Object.entries(workflow.jobs)) {
      for (const step of config.steps.filter(step => step.uses?.startsWith('actions/upload-artifact'))) {
        assert.equal(step.with['retention-days'], step.with.name.includes('logs') ? 7 : 30);
        assert.equal(step.with.overwrite, undefined);
      }
    }
  }
});

test('production checks blockers around approval; dry run has no remote writes or blockers', () => {
  for (const [job, expected] of [['validate', ['Check blockers before validation', 'Recheck blockers before approval']],
    ['release', ['Recheck blockers after approval']]]) {
    for (const name of expected) assert.equal(production.jobs[job].steps.find(s => s.name === name).run,
      'python3 scripts/release/pipeline.py blockers');
  }
  const release = dry.jobs.release;
  assert.equal(release.permissions.contents, 'read');
  assert.equal(release.steps[0].with['persist-credentials'], false);
  assert.ok(!JSON.stringify(dry).includes('pipeline.py push'));
  assert.ok(!JSON.stringify(dry).includes('pipeline.py github'));
  assert.ok(!JSON.stringify(dry).includes('pipeline.py blockers'));
  assert.ok(!JSON.stringify(dry).includes('--production'));
  assert.equal(dry.jobs.validate.steps.find(s => s.name === 'Validate release').with, undefined);
  assert.equal(release.steps.find(s => s.name === 'Publish to TestPyPI').with['repository-url'],
    'https://test.pypi.org/legacy/');
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
