/**
 * Applies the additive work-first mapping index used by global alias discovery.
 * Native deploys intentionally never execute migrations implicitly.
 *
 * Usage:
 * npx tsx scripts/apply-global-discovery-performance-index.ts --apply --confirm-global-discovery-index
 */
import dotenv from 'dotenv';
import { getYugabytePool } from '../src/db/yugabyte-direct.js';

dotenv.config({
  path: process.env.NOX_ENV_FILE || '/home/awerkori/.Projects/project-nox-importer/.env',
  override: false,
});

const apply = process.argv.includes('--apply');
const confirmed = process.argv.includes('--confirm-global-discovery-index');

async function main(): Promise<void> {
  if (!apply || !confirmed) {
    throw new Error('Refusing schema change without --apply --confirm-global-discovery-index');
  }
  const pool = getYugabytePool();
  try {
    // YugabyteDB has no concurrent index build. A short lock budget makes a
    // busy publication window a safe no-op retry rather than a content risk.
    await pool.query(`SET lock_timeout = '5s'`);
    await pool.query(`SET statement_timeout = '20s'`);
    await pool.query(`CREATE INDEX IF NOT EXISTS importer_work_mappings_v5_active_work_idx
      ON public.importer_work_mappings (work_id, source)
      WHERE work_id IS NOT NULL
        AND sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')`);
    const verification = await pool.query<{ indexname: string }>(`
      SELECT indexname FROM pg_indexes
      WHERE schemaname='public' AND tablename='importer_work_mappings'
        AND indexname='importer_work_mappings_v5_active_work_idx'`);
    if (verification.rowCount !== 1) throw new Error('Global-discovery active-work index was not visible after creation');
    console.log(JSON.stringify({ applied: true, index: verification.rows[0].indexname }));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(`Global discovery performance index failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
