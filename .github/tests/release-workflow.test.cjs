// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync, mkdtempSync, writeFileSync, chmodSync, rmSync} = require('node:fs');
const {tmpdir} = require('node:os');
const {join, resolve} = require('node:path');
const {spawnSync} = require('node:child_process');
const {test} = require('node:test');

const root = resolve(__dirname, '../..');

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
