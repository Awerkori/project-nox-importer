#!/usr/bin/env node
/**
 * Produces the mechanical, reviewable part of the Importer Definitivo audit.
 *
 * GitHub supplies the PR intent/state/merge commit; the checked-out history
 * supplies the affected files.  Keeping this generated avoids maintaining a
 * hand-written 800+ row document that becomes inaccurate at the next merge.
 * It intentionally never reads deployment credentials or production data.
 */
import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

const repository = process.env.IMPORTER_AUDIT_REPOSITORY || 'Awerkori/project-nox-importer';
const argumentsWithoutFlags = process.argv.slice(2).filter((argument) => argument !== '--hydrate-files');
const hydrateFiles = process.argv.includes('--hydrate-files');
const output = resolve(argumentsWithoutFlags[0] || 'docs/importer-pr-catalog.csv');
const raw = execFileSync('gh', [
  'pr', 'list', '--repo', repository, '--state', 'all', '--limit', '1000',
  '--json', 'number,title,state,mergedAt,closedAt,mergeCommit,url',
], { encoding: 'utf8' });
const pullRequests = JSON.parse(raw).sort((left, right) => left.number - right.number);

const filesForCommit = (sha) => {
  if (!sha) return [];
  try {
    return execFileSync('git', ['diff-tree', '--no-commit-id', '--name-only', '-r', '--first-parent', sha], {
      encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'],
    }).split('\n').map((file) => file.trim()).filter(Boolean);
  } catch {
    return [];
  }
};

// A local clone can intentionally omit old rewritten merge objects.  Fetching
// the changed paths through GitHub's GraphQL endpoint fills only those gaps in
// batches, without requiring a broad history rewrite or a PR-by-PR browser
// session.  A rare larger PR falls back to REST pagination so the catalogue
// remains complete rather than silently truncating the affected-file field.
const hydrateMissingFiles = (entries) => {
  if (!hydrateFiles) return;
  const [owner, name] = repository.split('/');
  const missing = entries.filter((entry) => entry.files.length === 0);
  for (let offset = 0; offset < missing.length; offset += 20) {
    const batch = missing.slice(offset, offset + 20);
    const fields = batch.map((entry) => `pr${entry.pr.number}: pullRequest(number: ${entry.pr.number}) {
      files(first: 100) { nodes { path } pageInfo { hasNextPage } }
    }`).join('\n');
    const query = `query { repository(owner: \"${owner}\", name: \"${name}\") { ${fields} } }`;
    const response = JSON.parse(execFileSync('gh', ['api', 'graphql', '-f', `query=${query}`], { encoding: 'utf8' }));
    for (const entry of batch) {
      const files = response?.data?.repository?.[`pr${entry.pr.number}`]?.files;
      if (!files) continue;
      entry.files = files.nodes.map((node) => node.path);
      if (files.pageInfo?.hasNextPage) {
        entry.files = execFileSync('gh', [
          'api', `repos/${repository}/pulls/${entry.pr.number}/files`, '--paginate', '--jq', '.[].filename',
        ], { encoding: 'utf8' }).split('\n').map((file) => file.trim()).filter(Boolean);
      }
    }
  }
};

const componentFor = (title, files) => {
  const corpus = `${title}\n${files.join('\n')}`.toLowerCase();
  const components = [];
  if (/src\/lite\/(runtime|repository|executor)|scheduler|claim|fair|priority|p0|p1|p2|lease/.test(corpus)) components.push('scheduler');
  if (/executor|media|download|upload|buffer|telegram|page/.test(corpus)) components.push('media');
  if (/discovery|catalog|metadata|alias|source/.test(corpus)) components.push('discovery');
  if (/ysql|postgres|migration|database|pool|sql/.test(corpus)) components.push('ysql');
  if (/publish|reader|chapter|canonical|gap|order/.test(corpus)) components.push('publication');
  if (/discloud|build_id|package-discloud|config|deploy/.test(corpus)) components.push('runtime');
  return components.length ? components.join('+') : 'other';
};

const relationFor = (pr) => {
  if (pr.number === 879) return 'reverts #878';
  const reverted = pr.title.match(/revert.*#(\d+)/i)?.[1];
  return reverted ? `reverts #${reverted}` : '';
};
const csv = (value) => `"${String(value ?? '').replaceAll('"', '""')}"`;
const entries = pullRequests.map((pr) => ({ pr, files: filesForCommit(pr.mergeCommit?.oid || '') }));
hydrateMissingFiles(entries);
const rows = [[
  'number', 'state', 'merged_at', 'closed_at', 'merge_commit', 'title',
  'components', 'affected_files', 'relation', 'url',
].join(',')];

for (const { pr, files } of entries) {
  const commit = pr.mergeCommit?.oid || '';
  rows.push([
    pr.number,
    pr.state,
    pr.mergedAt || '',
    pr.closedAt || '',
    commit,
    pr.title,
    componentFor(pr.title, files),
    files.join(';'),
    relationFor(pr),
    pr.url,
  ].map(csv).join(','));
}

mkdirSync(dirname(output), { recursive: true });
writeFileSync(output, `${rows.join('\n')}\n`);
console.log(`Wrote ${pullRequests.length} PRs to ${output}`);
