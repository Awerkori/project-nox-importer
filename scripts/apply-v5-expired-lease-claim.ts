/**
 * Applies the source-metadata claim function that recovers expired leases.
 * Native deploys intentionally never execute migrations implicitly.
 *
 * Usage:
 * npx tsx scripts/apply-v5-expired-lease-claim.ts --apply --confirm-v5-expired-lease-claim
 */
import dotenv from 'dotenv';
import { getYugabytePool } from '../src/db/yugabyte-direct.js';

dotenv.config({
  path: process.env.NOX_ENV_FILE || '/home/awerkori/.Projects/project-nox-importer/.env',
  override: false,
});

const apply = process.argv.includes('--apply');
const confirmed = process.argv.includes('--confirm-v5-expired-lease-claim');

async function main(): Promise<void> {
  if (!apply || !confirmed) throw new Error('Refusing function update without --apply --confirm-v5-expired-lease-claim');
  const pool = getYugabytePool();
  try {
    await pool.query(`SET lock_timeout = '5s'`);
    await pool.query(`SET statement_timeout = '20s'`);
    await pool.query(`CREATE OR REPLACE FUNCTION public.importer_lite_claim_source_metadata_task(
      p_worker_id text,p_lease interval DEFAULT interval '2 minutes'
    ) RETURNS SETOF public.importer_lite_tasks LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
    DECLARE v_task_id uuid; v_work_key text;
    BEGIN
      SELECT id,work_key INTO v_task_id,v_work_key FROM public.importer_lite_tasks
      WHERE task_type='CATALOG' AND monitoring=false AND payload ? 'metadataSourceRefresh'
        AND ((state IN ('READY','RETRY_WAIT') AND next_run_at<=now()) OR (state='RUNNING' AND lease_until<now()))
      ORDER BY next_run_at,created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED;
      IF NOT FOUND THEN RETURN; END IF;
      RETURN QUERY UPDATE public.importer_lite_tasks
        SET state='RUNNING',lease_owner=p_worker_id,lease_until=now()+p_lease,attempts=attempts+1,updated_at=now()
        WHERE id=v_task_id RETURNING *;
      PERFORM public.importer_lite_refresh_ready_work(v_work_key);
    END;
    $$`);
    console.log(JSON.stringify({ applied: true, function: 'importer_lite_claim_source_metadata_task' }));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(`V5 expired-lease claim update failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
