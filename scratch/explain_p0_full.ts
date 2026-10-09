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
        candidate_works AS MATERIALIZED (
          SELECT
            q.payload->>'workId' AS work_id,
            MIN(q.chapter_sort_key) as min_chapter_sort_key,
            MIN(q.next_run_at) as min_next_run_at
          FROM importer_queue q
          JOIN eligible_sources s ON s.id = q.source
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
            AND q.attempts < COALESCE(q.max_attempts, 7)
            AND q.priority >= 100 AND q.priority < 1000
          GROUP BY q.payload->>'workId'
          ORDER BY MIN(q.next_run_at) ASC, MIN(q.chapter_sort_key) ASC NULLS LAST
          LIMIT 100
        )
        SELECT q.payload->>'workId' AS work_id
        FROM candidate_works cw
        JOIN importer_queue q
          ON (q.payload->>'workId') = cw.work_id
          AND q.chapter_sort_key = cw.min_chapter_sort_key
          LEFT JOIN LATERAL (
            SELECT MAX(c.number) AS max_published
            FROM chapters c
            WHERE c.work_id = (q.payload->>'workId')::uuid
              AND c.published_at IS NOT NULL
          ) pub ON TRUE
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
            AND (pub.max_published IS NULL OR q.chapter_sort_key <= pub.max_published + 10)
          GROUP BY q.payload->>'workId'
          ORDER BY MIN(cw.min_next_run_at) ASC, MIN(cw.min_chapter_sort_key) ASC NULLS LAST
          LIMIT 32;
  `;

  console.log("Running EXPLAIN ANALYZE for full P0 query:");
  const res = await pool.query(query);
  console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  
  await pool.end();
}
run();
