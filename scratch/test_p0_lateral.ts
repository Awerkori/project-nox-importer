import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

async function main() {
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
        AND (s.id NOT IN (
          SELECT unnest(string_to_array(current_setting('app.active_sources', true), ','))
        ))
    ),
    candidate_works AS MATERIALIZED (
      SELECT q.payload->>'workId' AS work_id, q.chapter_sort_key, q.next_run_at
      FROM eligible_sources s
      CROSS JOIN LATERAL (
        SELECT q.payload, q.chapter_sort_key, q.next_run_at
        FROM importer_queue q
        WHERE q.source = s.id
          AND q.task_type = 'IMPORT_CHAPTER'
          AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND q.priority >= 100 AND q.priority < 1000
        ORDER BY q.next_run_at ASC, q.chapter_sort_key ASC NULLS LAST
        LIMIT 100
      ) q
    )
    SELECT
      work_id,
      MIN(chapter_sort_key) as min_chapter_sort_key,
      MIN(next_run_at) as min_next_run_at
    FROM candidate_works
    GROUP BY work_id
    ORDER BY min_next_run_at ASC, min_chapter_sort_key ASC NULLS LAST
    LIMIT 100;
  `;
  try {
    const res = await pool.query(query);
    console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
main();
