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
      ORDER BY s.id ASC
      LIMIT 8
    ),
    source_window AS MATERIALIZED (
      SELECT candidate.id
      FROM eligible_sources s
      CROSS JOIN LATERAL (
        SELECT q.id
        FROM importer_queue q
        WHERE q.source = s.id
          AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
          AND q.task_type = 'IMPORT_CHAPTER'
          AND q.attempts < COALESCE(q.max_attempts, 7)
        ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
        LIMIT 64
      ) candidate
    ),
    to_lock_filtered AS (
      SELECT q.id, q.payload, q.status, q.task_type, q.attempts, q.max_attempts, q.next_run_at, q.priority, q.chapter_sort_key, q.source
      FROM source_window windowed
      JOIN importer_queue q ON q.id = windowed.id
      JOIN works w ON w.id = (q.payload->>'workId')::uuid
      WHERE w.published = true
    ),
    to_lock AS (
      SELECT q_base.id
      FROM to_lock_filtered q
      JOIN importer_queue q_base ON q_base.id = q.id
      JOIN importer_sources s ON s.id = q.source
      LEFT JOIN LATERAL (
        SELECT MAX(c.number) AS max_published
        FROM chapters c
        WHERE c.work_id = (q.payload->>'workId')::uuid
          AND c.published_at IS NOT NULL
      ) pub ON TRUE
      WHERE (
        q.status = 'QUEUED'
        OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
      )
        AND q.task_type = 'IMPORT_CHAPTER'
        AND q.attempts < COALESCE(q.max_attempts, 7)
        AND (q.payload->>'chapterNumber')::float > COALESCE(pub.max_published, -1)
      ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
      FOR UPDATE OF q_base SKIP LOCKED
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
