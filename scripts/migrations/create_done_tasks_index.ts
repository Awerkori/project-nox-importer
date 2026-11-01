/**
 * Applies the bounded progressSnapshot index to a live YugabyteDB database.
 * Native deploys intentionally never execute migrations implicitly.
 *
 * Usage:
 *   npx tsx scripts/migrations/create_done_tasks_index.ts --apply --confirm-done-tasks-index
 */
import dotenv from 'dotenv';
import { getYugabytePool } from '../../src/db/yugabyte-direct.js';

dotenv.config({
  path: process.env.NOX_ENV_FILE || '/home/awerkori/.Projects/project-nox-importer/.env',
  override: false,
});

const apply = process.argv.includes('--apply');
const confirmed = process.argv.includes('--confirm-done-tasks-index');

async function main(): Promise<void> {
  if (!apply || !confirmed) {
    throw new Error('Refusing schema change without --apply --confirm-done-tasks-index');
  }

  const pool = getYugabytePool();
  try {
    await pool.query(`SET lock_timeout = '5s'`);
    await pool.query(`SET statement_timeout = '120s'`);
    await pool.query(`CREATE INDEX IF NOT EXISTS importer_lite_tasks_progress_unpublished_idx
      ON public.importer_lite_tasks (updated_at ASC)
      WHERE state = 'DONE'
        AND task_type = 'IMPORT_CHAPTER'
        AND checkpoint ? 'mediaIds'
        AND NOT checkpoint ? 'publishedAt'`);

    const verification = await pool.query<{ indexname: string }>(`
      SELECT indexname FROM pg_indexes
      WHERE schemaname='public' AND tablename='importer_lite_tasks'
        AND indexname='importer_lite_tasks_progress_unpublished_idx'`);
    console.log(JSON.stringify({ applied: true, indexes: verification.rows.map((row) => row.indexname) }));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(`Done tasks index failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
