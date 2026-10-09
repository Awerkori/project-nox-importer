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
    ),
    to_lock AS (
      SELECT q.id
      FROM importer_queue q
      JOIN works w ON w.id = (q.payload->>'workId')::uuid
      JOIN eligible_sources s ON s.id = q.source
      LEFT JOIN LATERAL (
        SELECT MAX(c.number) AS max_published
        FROM chapters c
        WHERE c.work_id = (q.payload->>'workId')::uuid
          AND c.published_at IS NOT NULL
      ) pub ON true
      WHERE (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
        AND q.task_type = 'IMPORT_CHAPTER'
        AND q.attempts < COALESCE(q.max_attempts, 7)
        AND w.published = true
        AND (q.payload->>'chapterNumber')::float > COALESCE(pub.max_published, -1)
      ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
      FOR UPDATE OF q SKIP LOCKED
      LIMIT 1
    )
    SELECT * FROM to_lock;
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
