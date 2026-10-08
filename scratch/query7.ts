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
  const res = await pool.query(`
    SELECT q.priority, w.published, COUNT(*) as cnt
    FROM importer_queue q
    JOIN importer_sources s ON q.source = s.id
    JOIN LATERAL (
      SELECT published FROM works w WHERE w.id = (q.payload->>'workId')::uuid
    ) w ON true
    WHERE q.status IN ('QUEUED', 'RETRY')
      AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
      AND q.task_type = 'IMPORT_CHAPTER'
      AND s.enabled = true
      AND (
        (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
        OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
      )
    GROUP BY 1, 2
    ORDER BY cnt DESC
  `);
  console.log(res.rows);
  await pool.end();
}
run().catch(console.error);
