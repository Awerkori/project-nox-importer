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
  const q = `EXPLAIN ANALYZE WITH candidate_works AS MATERIALIZED (
          SELECT 
            q.payload->>'workId' AS work_id,
            MIN(q.chapter_sort_key) as min_chapter_sort_key,
            MIN(q.next_run_at) as min_next_run_at
          FROM (
            SELECT payload, chapter_sort_key, next_run_at, source
            FROM importer_queue q
            WHERE q.task_type = 'IMPORT_CHAPTER'
              AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
              AND q.attempts < COALESCE(q.max_attempts, 7)
              AND q.priority >= 75 AND q.priority < 100
              AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
            ORDER BY q.priority DESC, q.chapter_sort_key ASC
            LIMIT 400
          ) q
          JOIN importer_sources s ON s.id = q.source
          JOIN works w ON w.id = (q.payload->>'workId')::uuid
          WHERE w.published IS TRUE
            AND s.enabled = true
            AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          GROUP BY q.payload->>'workId'
  )
  SELECT 1 FROM candidate_works`;
  
  const res = await pool.query(q);
  console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  await pool.end();
}
run().catch(console.error);
