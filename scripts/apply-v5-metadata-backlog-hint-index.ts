/**
 * Applies the additive V5 backlog-hint index. Native deploys deliberately do
 * not execute migrations implicitly.
 *
 * Usage:
 * npx tsx scripts/apply-v5-metadata-backlog-hint-index.ts --apply --confirm-v5-backlog-hint-index
 */
import dotenv from 'dotenv';
import { getYugabytePool } from '../src/db/yugabyte-direct.js';

dotenv.config({
  path: process.env.NOX_ENV_FILE || '/home/awerkori/.Projects/project-nox-importer/.env',
  override: false,
});

const apply = process.argv.includes('--apply');
const confirmed = process.argv.includes('--confirm-v5-backlog-hint-index');

async function main(): Promise<void> {
  if (!apply || !confirmed) {
    throw new Error('Refusing schema change without --apply --confirm-v5-backlog-hint-index');
  }
  const pool = getYugabytePool();
  try {
    // YugabyteDB has no concurrent index build. A bounded lock and statement
    // budget make a busy production window a safe no-op retry; no catalog
    // data, mapping or task is changed by this operation.
    await pool.query(`SET lock_timeout = '5s'`);
    await pool.query(`SET statement_timeout = '20s'`);
    await pool.query(`CREATE INDEX IF NOT EXISTS importer_lite_metadata_enrichment_v5_backlog_hint_idx
      ON public.importer_lite_metadata_enrichment (updated_at, source, source_work_id)
      WHERE version < 5`);
    const verification = await pool.query<{ indexname: string }>(`
      SELECT indexname FROM pg_indexes
      WHERE schemaname='public' AND tablename='importer_lite_metadata_enrichment'
        AND indexname='importer_lite_metadata_enrichment_v5_backlog_hint_idx'`);
    if (verification.rowCount !== 1) throw new Error('V5 backlog-hint index was not visible after creation');
    console.log(JSON.stringify({ applied: true, index: verification.rows[0].indexname }));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(`V5 backlog-hint index failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
