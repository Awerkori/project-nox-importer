/**
 * Applies the additive V4 active-work partial index in production.
 *
 * Native deploys intentionally do not execute migrations. YugabyteDB lacks
 * concurrent index builds, so this runner refuses rather than waiting behind
 * a busy writer for more than five seconds. It creates no tasks and modifies
 * no importer, work, chapter, alias, tag, or publication row.
 *
 * Usage:
 * npx tsx scripts/apply-v4-metadata-performance-index.ts --apply --confirm-v4-active-work-index
 */
import dotenv from 'dotenv';
import { getYugabytePool } from '../src/db/yugabyte-direct.js';

dotenv.config({
  path: process.env.NOX_ENV_FILE || '/home/awerkori/.Projects/project-nox-importer/.env',
  override: false,
});

const apply = process.argv.includes('--apply');
const confirmed = process.argv.includes('--confirm-v4-active-work-index');

async function main(): Promise<void> {
  if (!apply || !confirmed) {
    throw new Error('Refusing schema change without --apply --confirm-v4-active-work-index');
  }
  const pool = getYugabytePool();
  try {
    await pool.query(`SET lock_timeout = '5s'`);
    await pool.query(`SET statement_timeout = '20s'`);
    await pool.query(`CREATE INDEX IF NOT EXISTS importer_lite_tasks_v4_active_work_idx
      ON public.importer_lite_tasks (work_id)
      WHERE task_type='CATALOG'
        AND state IN ('READY','RUNNING','RETRY_WAIT')
        AND payload->>'metadataEnrichmentVersion'='4'`);
    const verification = await pool.query<{ indexname: string; indexdef: string }>(
      `SELECT indexname,indexdef FROM pg_indexes
       WHERE schemaname='public' AND tablename='importer_lite_tasks'
         AND indexname='importer_lite_tasks_v4_active_work_idx'`,
    );
    if (verification.rowCount !== 1) throw new Error('V4 active-work index was not visible after creation');
    console.log(JSON.stringify({ applied: true, index: verification.rows[0].indexname }));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(`V4 metadata performance index failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
