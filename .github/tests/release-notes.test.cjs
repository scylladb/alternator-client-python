// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync, mkdtempSync, writeFileSync, chmodSync, rmSync} = require('node:fs');
const {tmpdir} = require('node:os');
const {join, resolve} = require('node:path');
const {spawnSync} = require('node:child_process');
const {test} = require('node:test');

const root = resolve(__dirname, '../..');
const classifier = readFileSync(join(root, '.github/workflows/label-release-notes.yml'), 'utf8');
const script = classifier.split('          script: |\n')[1]
  .split('\n').map(line => line.replace(/^            /, '')).join('\n');
const runScript = new (Object.getPrototypeOf(async function () {}).constructor)(
  'github', 'context', 'process', script
);

async function classify(paths, options = {}) {
  const files = paths.map(path => typeof path === 'string' ? {filename: path, status: 'modified'} : path);
  const calls = [];
  let reads = 0;
  const github = {
    rest: {
      pulls: {
        get: async () => ({data: {
          changed_files: options.count ?? files.length,
          head: {sha: options.changing && reads++ ? 'new' : 'head'},
          base: {sha: 'base'}
        }}),
        listFiles: Symbol('listFiles')
      },
      issues: {
        removeLabel: async () => {
          calls.push('remove');
          if (options.noLabel) throw {status: 404};
        },
        getLabel: async () => {
          calls.push('get');
          if (options.missingLabel) throw {status: 404};
        },
        createLabel: async () => calls.push('create'),
        addLabels: async () => calls.push('add')
      }
    },
    paginate: async (method, params) => {
      assert.equal(method, github.rest.pulls.listFiles);
      assert.equal(params.per_page, 100);
      if (options.apiFailure) throw new Error('API unavailable');
      return files;
    }
  };
  let error;
  try {
    await runScript(github, {repo: {owner: 'scylladb', repo: 'alternator-client-python'}},
      {env: {PR_NUMBER: options.number ?? '123'}});
  } catch (caught) { error = caught; }
  return {calls, error};
}

test('support-only and mixed support categories create and apply shared label', async () => {
  const {calls, error} = await classify(['docs/guide.md', 'tests/unit/test_config.py',
    'native/tests/test_extension.py', '.github/workflows/ci.yml', '.gitignore'], {missingLabel: true});
  assert.ifError(error);
  assert.deepEqual(calls, ['get', 'create', 'add']);
});

for (const path of ['alternator/config.py', 'pyproject.toml', 'native/pyproject.toml',
  'native/Cargo.toml', 'native/Cargo.lock', 'native/vendor/tokio/README.md',
  'LICENSE', 'Makefile', 'scripts/native/build.py', 'unknown.md']) {
  test(`production or unknown path removes stale label: ${path}`, async () => {
    const {calls, error} = await classify(['docs/guide.md', path]);
    assert.ifError(error);
    assert.deepEqual(calls, ['remove']);
  });
}

test('renames must stay within support paths', async () => {
  for (const [filename, previous_filename, expected] of [
    ['docs/new.md', 'docs/old.md', ['get', 'add']],
    ['tests/unit/new.py', 'alternator/old.py', ['remove']],
    ['alternator/new.py', 'tests/unit/old.py', ['remove']],
    ['docs/new.md', undefined, ['remove']]
  ]) {
    const {calls, error} = await classify([{filename, previous_filename, status: 'renamed'}]);
    assert.ifError(error);
    assert.deepEqual(calls, expected);
  }
});

test('pagination retains all 101 support files', async () => {
  const {calls, error} = await classify(Array.from({length: 101}, (_, n) => `tests/${n}.py`));
  assert.ifError(error);
  assert.deepEqual(calls, ['get', 'add']);
});

test('truncated, empty, duplicate, changing and failed listings remove label and fail', async () => {
  for (const [paths, options] of [
    [['docs/a.md'], {count: 3001}], [[], {}],
    [['docs/a.md', 'docs/a.md'], {}],
    [['docs/a.md'], {changing: true}], [['docs/a.md'], {apiFailure: true}]
  ]) {
    const {calls, error} = await classify(paths, options);
    assert.ok(error);
    assert.deepEqual(calls, ['remove']);
  }
});

test('production PR without existing omission label succeeds', async () => {
  const {calls, error} = await classify(['pyproject.toml'], {noLabel: true});
  assert.ifError(error);
  assert.deepEqual(calls, ['remove']);
});

test('workflow_dispatch validates PR number before API access', async () => {
  const {calls, error} = await classify(['README.md'], {number: '1; echo unsafe'});
  assert.ok(error);
  assert.deepEqual(calls, []);
});

test('published release rerun preserves existing assets', () => {
  const workflow = readFileSync(join(root, '.github/workflows/release.yml'), 'utf8');
  const publish = workflow.split('      - name: Create draft, attach artifacts, and publish release\n')[1]
    .split('        run: |\n')[1].split('\n').map(line => line.replace(/^          /, '')).join('\n');
  const dir = mkdtempSync(join(tmpdir(), 'release-notes-'));
  try {
    writeFileSync(join(dir, 'gh'), '#!/bin/sh\nif [ "$1 $2" = "release view" ]; then\n  echo \'{"isDraft":false,"url":"https://example.test/release"}\'\nelse\n  echo "Unexpected mutation" >&2\n  exit 1\nfi\n');
    chmodSync(join(dir, 'gh'), 0o755);
    const result = spawnSync('bash', ['-c', publish], {encoding: 'utf8', env: {
      ...process.env, PATH: `${dir}:${process.env.PATH}`, RELEASE_VERSION: '2.0.2', PRERELEASE: 'false'
    }});
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /already published/);
  } finally { rmSync(dir, {recursive: true}); }
});


test('tag verification rejects a release tag pointing at another commit', () => {
  const workflow = readFileSync(join(root, '.github/workflows/release.yml'), 'utf8');
  const verify = workflow.split('      - name: Verify published tag identifies the signed release commit\n')[1]
    .split('        run: |\n')[1].split('\n      - name:')[0]
    .split('\n').map(line => line.replace(/^          /, '')).join('\n');
  const dir = mkdtempSync(join(tmpdir(), 'release-tag-'));
  try {
    writeFileSync(join(dir, 'git'), '#!/bin/sh\nif [ "$1" = "rev-parse" ]; then echo signed-commit; fi\n');
    chmodSync(join(dir, 'git'), 0o755);
    for (const [commit, status] of [['signed-commit', 0], ['wrong-commit', 1]]) {
      const result = spawnSync('bash', ['-c', verify], {encoding: 'utf8', env: {
        ...process.env, PATH: `${dir}:${process.env.PATH}`,
        RELEASE_VERSION: '2.0.2', RELEASE_COMMIT: commit
      }});
      assert.equal(result.status, status, result.stderr);
    }
  } finally { rmSync(dir, {recursive: true}); }
});
