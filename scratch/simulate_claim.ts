import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

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
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  const p0Res = await pool.query(`
    SELECT DISTINCT q.payload->>'workId' as work_id
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    JOIN importer_sources s ON q.source = s.id
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
      AND q.priority >= 50
      AND s.enabled = true
      AND w.published = true
      AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
    LIMIT 200
  `);
  console.log("P0 candidates:", p0Res.rows.length);

  const p1Res = await pool.query(`
    SELECT DISTINCT q.payload->>'workId' as work_id
    FROM importer_queue q
    JOIN importer_sources s ON q.source = s.id
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
      AND q.priority >= 50
      AND s.enabled = true
      AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
    LIMIT 100
  `);
  console.log("P1 candidates:", p1Res.rows.length);

  await pool.end();
}
run();
