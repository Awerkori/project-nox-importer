import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

async function run() {
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  const query = `
    EXPLAIN ANALYZE
    WITH eligible_sources AS MATERIALIZED (
      SELECT s.id
      FROM importer_sources s
      WHERE s.enabled = true
        AND (
          (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
          OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
        )
    ),
    candidate_works AS (
      SELECT
        q.payload->>'workId' AS work_id,
        MIN(q.chapter_sort_key) as min_chapter_sort_key,
        MIN(q.next_run_at) as min_next_run_at
      FROM importer_queue q
      WHERE q.task_type = 'IMPORT_CHAPTER'
        AND q.status IN ('QUEUED', 'RETRY')
        AND q.source = ANY (ARRAY(SELECT id FROM eligible_sources))
      GROUP BY q.payload->>'workId'
    )
    SELECT work_id FROM candidate_works LIMIT 10;
  `;

  console.log("Running EXPLAIN ANALYZE for ANY (ARRAY(...)) rewrite:");
  const res = await pool.query(query);
  console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  
  await pool.end();
}
run();
