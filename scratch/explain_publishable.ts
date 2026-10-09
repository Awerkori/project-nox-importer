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
  WITH staged_works AS (
    SELECT
      m.work_id,
      MIN(m.chapter_sort_key) as frontier_sort_key
    FROM importer_chapter_mappings m
    WHERE m.status IN ('STAGED', 'WAITING_FOR_GAP') AND m.work_id IS NOT NULL
    GROUP BY m.work_id
  ),
  works_with_published AS (
    SELECT
      sw.work_id,
      sw.frontier_sort_key,
      (
        SELECT MAX(c.number)
        FROM chapters c
        WHERE c.work_id = sw.work_id
          AND c.published_at IS NOT NULL
      ) as max_published,
      EXISTS (
        SELECT 1
        FROM importer_chapter_mappings pm
        WHERE pm.work_id = sw.work_id
          AND pm.chapter_sort_key < sw.frontier_sort_key
          AND pm.is_gap = false
          AND pm.status NOT IN ('STAGED', 'WAITING_FOR_GAP')
      ) as has_predecessor_in_mapping,
      EXISTS (
        SELECT 1
        FROM importer_queue pq
        WHERE (pq.payload->>'workId') = sw.work_id::text
          AND pq.task_type = 'IMPORT_CHAPTER'
          AND pq.status IN ('QUEUED', 'RETRY', 'IMPORTING')
          AND pq.chapter_sort_key < sw.frontier_sort_key
      ) as has_predecessor_in_queue
    FROM staged_works sw
  )
  SELECT work_id
  FROM works_with_published
  WHERE (max_published IS NOT NULL AND frontier_sort_key <= max_published + 1.05 AND NOT has_predecessor_in_queue)
     OR (max_published IS NULL AND NOT has_predecessor_in_mapping AND NOT has_predecessor_in_queue)
  ORDER BY frontier_sort_key ASC, work_id ASC
  LIMIT 20;
  `;

  const res = await pool.query(query);
  console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  
  await pool.end();
}
run();
