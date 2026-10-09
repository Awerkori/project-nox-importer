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
  WITH staged_works AS MATERIALIZED (
    SELECT
      m.work_id,
      MIN(m.chapter_sort_key) as frontier_sort_key
    FROM importer_chapter_mappings m
    WHERE m.status IN ('STAGED', 'WAITING_FOR_GAP') AND m.work_id IS NOT NULL
    GROUP BY m.work_id
  ),
  published_max AS MATERIALIZED (
    SELECT c.work_id, MAX(c.number) as max_published
    FROM chapters c
    JOIN staged_works sw ON c.work_id = sw.work_id
    WHERE c.published_at IS NOT NULL
    GROUP BY c.work_id
  ),
  mapping_preds AS MATERIALIZED (
    SELECT pm.work_id, true as has_mapping_pred
    FROM importer_chapter_mappings pm
    JOIN staged_works sw ON pm.work_id = sw.work_id
    WHERE pm.chapter_sort_key < sw.frontier_sort_key
      AND pm.is_gap = false
      AND pm.status NOT IN ('STAGED', 'WAITING_FOR_GAP')
    GROUP BY pm.work_id
  ),
  queue_preds AS MATERIALIZED (
    SELECT (pq.payload->>'workId')::uuid as work_id, true as has_queue_pred
    FROM importer_queue pq
    JOIN staged_works sw ON (pq.payload->>'workId') = sw.work_id::text
    WHERE pq.task_type = 'IMPORT_CHAPTER'
      AND pq.status IN ('QUEUED', 'RETRY', 'IMPORTING')
      AND pq.chapter_sort_key < sw.frontier_sort_key
    GROUP BY pq.payload->>'workId'
  )
  SELECT sw.work_id
  FROM staged_works sw
  LEFT JOIN published_max pub ON pub.work_id = sw.work_id
  LEFT JOIN mapping_preds mp ON mp.work_id = sw.work_id
  LEFT JOIN queue_preds qp ON qp.work_id = sw.work_id
  WHERE (pub.max_published IS NOT NULL AND sw.frontier_sort_key <= pub.max_published + 1.05 AND qp.has_queue_pred IS NULL)
     OR (pub.max_published IS NULL AND mp.has_mapping_pred IS NULL AND qp.has_queue_pred IS NULL)
  ORDER BY sw.frontier_sort_key ASC, sw.work_id ASC
  LIMIT 20;
  `;

  const res = await pool.query(query);
  console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  
  await pool.end();
}
run();
