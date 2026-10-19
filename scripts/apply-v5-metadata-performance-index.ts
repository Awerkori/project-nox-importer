/**
 * Applies the additive active-mapping index used by V5 source metadata
 * admission. Native deploys never execute migrations implicitly.
 *
 * Usage:
 * npx tsx scripts/apply-v5-metadata-performance-index.ts --apply --confirm-v5-active-mapping-index
 */
import dotenv from 'dotenv';
import { getYugabytePool } from '../src/db/yugabyte-direct.js';

dotenv.config({
  path: process.env.NOX_ENV_FILE || '/home/awerkori/.Projects/project-nox-importer/.env',
  override: false,
});

const apply = process.argv.includes('--apply');
const confirmed = process.argv.includes('--confirm-v5-active-mapping-index');

async function main(): Promise<void> {
  if (!apply || !confirmed) {
    throw new Error('Refusing schema change without --apply --confirm-v5-active-mapping-index');
  }
  const pool = getYugabytePool();
  try {
    // YugabyteDB index creation is atomic but not concurrent. The short lock
    // budget makes a busy production writer a safe retry rather than a
    // publication stall; this operation changes no importer data.
    await pool.query(`SET lock_timeout = '5s'`);
    await pool.query(`SET statement_timeout = '20s'`);
    await pool.query(`CREATE INDEX IF NOT EXISTS importer_work_mappings_v5_active_source_work_idx
      ON public.importer_work_mappings (source, source_work_id, work_id)
      WHERE work_id IS NOT NULL
        AND sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')`);
    const verification = await pool.query<{ indexname: string }>(`
      SELECT indexname FROM pg_indexes
      WHERE schemaname='public' AND tablename='importer_work_mappings'
        AND indexname='importer_work_mappings_v5_active_source_work_idx'`);
    if (verification.rowCount !== 1) throw new Error('V5 active-mapping index was not visible after creation');
    console.log(JSON.stringify({ applied: true, index: verification.rows[0].indexname }));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(`V5 metadata performance index failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
