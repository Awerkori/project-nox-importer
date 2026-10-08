import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

const SOURCE_EXECUTION_ELIGIBILITY_SQL = `(
  (
    s.status = 'ACTIVE'
    AND (
      s.blocked_reason IS NULL
      OR s.blocked_details->>'probe_success' = 'true'
      OR s.blocked_details->>'recovered_at' IS NOT NULL
    )
  )
  OR (
    s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED')
    AND (
      s.blocked_reason IS NULL
      OR s.blocked_details->>'probe_success' = 'true'
      OR s.blocked_details->>'recovered_at' IS NOT NULL
    )
    AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())
  )
)`;

async function run() {
  const resP2 = await pool.query(`
      SELECT q.id, q.source, q.priority, q.chapter_sort_key, q.payload->>'workId' as work_id
      FROM importer_queue q
      JOIN importer_sources s ON s.id = q.source
      JOIN LATERAL (
        SELECT published FROM works w WHERE w.id = (q.payload->>'workId')::uuid
      ) w ON true
      WHERE q.task_type = 'IMPORT_CHAPTER'
        AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
        AND q.priority >= 50 AND q.priority < 75
        AND w.published IS FALSE
        AND s.enabled = true
        AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
      LIMIT 10
  `);
  console.log('P2 Canonical Result:', resP2.rows);

  if (resP2.rows.length > 0) {
    const job = resP2.rows[0];
    const blockRes = await pool.query(`
      SELECT id, status, chapter_sort_key 
      FROM importer_queue
      WHERE (payload->>'workId') = $1
        AND task_type = 'IMPORT_CHAPTER'
        AND chapter_sort_key < $2
        AND status IN ('QUEUED', 'RETRY', 'IMPORTING')
    `, [job.work_id, job.chapter_sort_key]);
    console.log('Blockers for', job.id, ':', blockRes.rows);
  }

  await pool.end();
}
run().catch(console.error);
