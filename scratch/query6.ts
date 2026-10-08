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

async function run() {
  const qRes = await pool.query(`
      SELECT 
        COUNT(CASE WHEN q.status IN ('QUEUED', 'RETRY') AND (q.next_run_at IS NULL OR q.next_run_at <= NOW()) AND q.task_type = 'IMPORT_CHAPTER' AND s.enabled = true AND (
          (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
          OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
        ) THEN 1 END) as eligible_cnt,
        COUNT(CASE WHEN q.status = 'IMPORTING' THEN 1 END) as importing_cnt,
        COUNT(CASE WHEN q.status = 'RETRY' THEN 1 END) as retry_cnt
      FROM importer_queue q
      LEFT JOIN importer_sources s ON q.source = s.id
      WHERE q.status IN ('QUEUED', 'RETRY', 'IMPORTING')
  `);
  console.log(qRes.rows[0]);
  await pool.end();
}
run().catch(console.error);
