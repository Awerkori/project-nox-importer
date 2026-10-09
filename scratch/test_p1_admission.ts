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
    ), queued_retry AS MATERIALIZED (
      SELECT q.*
      FROM importer_queue q
      JOIN eligible_sources s ON s.id = q.source
      WHERE q.task_type = 'IMPORT_CHAPTER'
        AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
        AND q.attempts < COALESCE(q.max_attempts,7)
        AND q.priority >= 75 AND q.priority < 100
        AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
        AND q.payload->>'workId' IS NOT NULL
      ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
      LIMIT 160
    )
    SELECT * FROM queued_retry;
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
