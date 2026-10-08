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
        EXPLAIN ANALYZE WITH eligible_sources AS MATERIALIZED (
          SELECT id FROM importer_sources WHERE enabled = true
        ), queue_candidates AS MATERIALIZED (
          SELECT q.payload->>'workId' AS work_id, q.source, COUNT(*) AS pending_jobs,
            COUNT(*) FILTER (WHERE q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())) AS queued_count,
            COUNT(*) FILTER (WHERE q.status = 'PAUSED_BY_STAFF') AS paused_count,
            MIN(q.chapter_sort_key) AS min_sort_key
          FROM (
            SELECT q.* FROM importer_queue q
            JOIN eligible_sources s ON s.id = q.source
            WHERE q.task_type = 'IMPORT_CHAPTER'
              AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
              AND q.attempts < COALESCE(q.max_attempts,7)
              AND q.priority >= 75 AND q.priority < 100
              AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
              AND q.payload->>'workId' IS NOT NULL
            ORDER BY q.priority DESC, q.chapter_sort_key ASC
            LIMIT 50
          ) q
          GROUP BY q.payload->>'workId', q.source
        )
        SELECT * FROM queue_candidates;
  `;
  const res = await pool.query(query);
  for (const row of res.rows) {
    console.log(row['QUERY PLAN']);
  }
  await pool.end();
}
run();
