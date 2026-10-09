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

  const res = await pool.query(`
  WITH staged_works AS (
    SELECT
      m.work_id,
      MIN(m.chapter_sort_key) as frontier_sort_key
    FROM importer_chapter_mappings m
    WHERE m.status IN ('STAGED', 'WAITING_FOR_GAP') AND m.work_id IS NOT NULL
    GROUP BY m.work_id
  )
  SELECT 
    sw.work_id,
    sw.frontier_sort_key,
    (SELECT MAX(c.number) FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL) as max_pub,
    (SELECT 1 FROM importer_chapter_mappings pm WHERE pm.work_id = sw.work_id AND pm.chapter_sort_key < sw.frontier_sort_key AND pm.is_gap = false AND pm.status NOT IN ('STAGED', 'WAITING_FOR_GAP') LIMIT 1) as has_mapping_pred,
    (SELECT 1 FROM importer_queue pq WHERE (pq.payload->>'workId') = sw.work_id::text AND pq.task_type = 'IMPORT_CHAPTER' AND pq.status IN ('QUEUED', 'RETRY', 'IMPORTING') AND pq.chapter_sort_key < sw.frontier_sort_key LIMIT 1) as has_queue_pred
  FROM staged_works sw LIMIT 10;
  `);
  console.log(res.rows);
  
  for (const r of res.rows) {
      if (r.has_mapping_pred) {
          const m = await pool.query(`SELECT id, status, is_gap, chapter_sort_key FROM importer_chapter_mappings pm WHERE pm.work_id = $1 AND pm.chapter_sort_key < $2 AND pm.is_gap = false AND pm.status NOT IN ('STAGED', 'WAITING_FOR_GAP') ORDER BY chapter_sort_key DESC LIMIT 3`, [r.work_id, r.frontier_sort_key]);
          console.log(`Mapping pred for ${r.work_id}:`, m.rows);
      }
      if (r.has_queue_pred) {
          const q = await pool.query(`SELECT status, task_type, priority, next_run_at FROM importer_queue pq WHERE pq.payload->>'workId' = $1 AND pq.task_type = 'IMPORT_CHAPTER' AND pq.status IN ('QUEUED', 'RETRY', 'IMPORTING') AND pq.chapter_sort_key < $2 ORDER BY chapter_sort_key DESC LIMIT 3`, [r.work_id, r.frontier_sort_key]);
          console.log(`Queue pred for ${r.work_id}:`, q.rows);
      }
  }

  await pool.end();
}
run();
