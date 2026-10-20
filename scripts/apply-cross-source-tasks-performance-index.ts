/**
 * Applies the partial indexes on importer_lite_tasks used by cross-source metadata discovery.
 * Native deploys intentionally never execute migrations implicitly.
 *
 * Usage:
 * npx tsx scripts/apply-cross-source-tasks-performance-index.ts --apply --confirm-cross-source-index
 */
import dotenv from 'dotenv';
import { getYugabytePool } from '../src/db/yugabyte-direct.js';

dotenv.config({
  path: process.env.NOX_ENV_FILE || '/home/awerkori/.Projects/project-nox-importer/.env',
  override: false,
});

const apply = process.argv.includes('--apply');
const confirmed = process.argv.includes('--confirm-cross-source-index');

async function main(): Promise<void> {
  if (!apply || !confirmed) {
    throw new Error('Refusing schema change without --apply --confirm-cross-source-index');
  }
  const pool = getYugabytePool();
  try {
    await pool.query(`SET lock_timeout = '5s'`);
    await pool.query(`SET statement_timeout = '20s'`);
    
    await pool.query(`CREATE INDEX IF NOT EXISTS importer_lite_tasks_cross_source_active_idx
      ON public.importer_lite_tasks (next_run_at, id)
      WHERE task_type='CATALOG'
        AND state IN ('READY','RUNNING','RETRY_WAIT')
        AND payload ? 'metadataCrossSourceDiscovery'`);

    await pool.query(`CREATE INDEX IF NOT EXISTS importer_lite_tasks_cross_source_deferred_idx
      ON public.importer_lite_tasks (next_run_at, id)
      WHERE task_type='CATALOG' AND state='DONE'
        AND payload ? 'metadataCrossSourceDiscovery'
        AND checkpoint->>'coverage'='CROSS_SOURCE_PARTIAL_EXTERNAL_DEFERRED'`);

    const verification = await pool.query<{ indexname: string }>(`
      SELECT indexname FROM pg_indexes
      WHERE schemaname='public' AND tablename='importer_lite_tasks'
        AND indexname IN ('importer_lite_tasks_cross_source_active_idx', 'importer_lite_tasks_cross_source_deferred_idx')
      ORDER BY indexname`);
    console.log(JSON.stringify({ applied: true, indexes: verification.rows.map(r => r.indexname) }));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(`Cross source tasks index failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
