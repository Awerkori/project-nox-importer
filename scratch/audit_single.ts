import 'dotenv/config';
import fs from 'fs';
import pg from 'pg';
import { SourceRegistry } from '../src/sources/registry.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { auditWork } from '../scripts/audit_full_catalog_daemon.js';

async function main() {
  const pool = new pg.Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  const limiter = new HostRateLimiter(8.0);
  const registry = new SourceRegistry(limiter);
  const sources = registry.getAll();

  const targetWorkId = process.argv[2] || '602fd450-a258-46f2-b422-6500e28ba3c4';

  const [wRes, mRes, tRes] = await Promise.all([
    pool.query('SELECT id::text, title, slug, aliases FROM works WHERE id = $1::uuid', [targetWorkId]),
    pool.query('SELECT work_id::text, source, source_work_id, source_title, source_slug FROM importer_work_mappings WHERE work_id = $1::uuid', [targetWorkId]),
    pool.query("SELECT work_id::text, id::text as v7_task_id, state as v7_state, updated_at as v7_updated_at, checkpoint as v7_checkpoint FROM importer_lite_tasks WHERE work_id = $1::uuid AND task_type = 'CATALOG' AND payload->>'metadataEnrichmentVersion' = '7'", [targetWorkId]),
  ]);

  const w = wRes.rows[0];
  const mappings = mRes.rows;
  const taskInfo = tRes.rows[0];

  console.log('Auditing work:', w.title, '(id:', w.id, ')');
  const record = await auditWork(w, sources, mappings, w.aliases || [], taskInfo, null);
  console.log('=== AUDIT RESULT ===');
  console.log('Verdict:', record.verdict);
  console.log('missingFromYsql:', record.missingFromYsql);
  console.log('isBug:', record.isBug);
  console.log('operationalCoverage:', record.operationalCoverage);
  console.log('foundSourcesCount:', record.foundSourcesCount);

  // Append fresh record to scratch/post_v7_audit_results.jsonl
  const outFile = '/home/awerkori/.Projects/project-nox-importer/scratch/post_v7_audit_results.jsonl';
  fs.appendFileSync(outFile, JSON.stringify(record) + '\n');
  console.log('Appended fresh record to', outFile);

  await pool.end();
}
main().catch(console.error);
