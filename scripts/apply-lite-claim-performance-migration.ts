/**
 * Applies the additive production form of the fair ready-work claim index.
 *
 * Usage:
 *   npx tsx scripts/apply-lite-claim-performance-migration.ts --apply --confirm-lite-claim-index
 *
 * It creates no tasks, touches no importer rows and is idempotent.  The
 * YugabyteDB does not implement concurrent index builds. Its normal index
 * build is atomic; the five-second lock budget means a busy writer causes a
 * safe refusal rather than an unbounded publication delay. Keep it separate
 * from app startup: native deploys never run migrations implicitly.
 */
import dotenv from 'dotenv';
import { getYugabytePool } from '../src/db/yugabyte-direct.js';

dotenv.config({
  path: process.env.NOX_ENV_FILE || '/home/awerkori/.Projects/project-nox-importer/.env',
  override: false,
});

const apply = process.argv.includes('--apply');
const confirmed = process.argv.includes('--confirm-lite-claim-index');

async function main(): Promise<void> {
  if (!apply || !confirmed) {
    throw new Error('Refusing schema change without --apply --confirm-lite-claim-index');
  }
  const pool = getYugabytePool();
  try {
    await pool.query(`SET lock_timeout = '5s'`);
    await pool.query(`SET statement_timeout = '20s'`);
    await pool.query(`CREATE INDEX IF NOT EXISTS importer_lite_ready_works_fair_claim_idx
      ON public.importer_lite_ready_works
        (priority, operation_rank, last_claimed_at NULLS FIRST, next_run_at, work_key)`);
    const verification = await pool.query<{ indexname: string; indexdef: string }>(
      `SELECT indexname,indexdef FROM pg_indexes
       WHERE schemaname='public' AND tablename='importer_lite_ready_works'
         AND indexname='importer_lite_ready_works_fair_claim_idx'`,
    );
    if (verification.rowCount !== 1) throw new Error('Fair ready-work claim index was not visible after creation');
    console.log(JSON.stringify({ applied: true, index: verification.rows[0].indexname }));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(`Lite claim performance migration failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
