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
    WITH candidate_works AS MATERIALIZED (
      SELECT 
        q.payload->>'workId' AS work_id,
        MIN(q.chapter_sort_key) as min_chapter_sort_key,
        MIN(q.next_run_at) as min_next_run_at
      FROM importer_queue q
      JOIN importer_sources s ON s.id = q.source
      JOIN works w ON w.id = (q.payload->>'workId')::uuid
      WHERE q.task_type = 'IMPORT_CHAPTER'
        AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
        AND w.published IS TRUE
        AND q.attempts < COALESCE(q.max_attempts, 7)
        AND q.priority >= 75 AND q.priority < 100
        AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
        AND s.enabled = true
      GROUP BY q.payload->>'workId'
      ORDER BY MIN(q.next_run_at) ASC, MIN(q.chapter_sort_key) ASC NULLS LAST
      LIMIT 100
    )
    SELECT * FROM candidate_works;
  `;

  try {
    const res = await pool.query(query);
    console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  } catch(e) {
    console.error(e);
  }
  await pool.end();
}
run();
