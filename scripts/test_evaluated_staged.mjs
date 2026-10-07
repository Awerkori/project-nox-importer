import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: {
    rejectUnauthorized: true,
    ca: fs.readFileSync('./config/root.crt').toString()
  }
});

async function run() {
  const client = await pool.connect();
  try {
    const q = `
      WITH staged_summary AS (
        SELECT 
          m.work_id,
          MIN(m.chapter_sort_key) as frontier_sort_key,
          COUNT(DISTINCT m.chapter_sort_key) as total_staged_chapters
        FROM importer_chapter_mappings m
        WHERE m.status IN ('STAGED', 'WAITING_FOR_GAP') AND m.work_id IS NOT NULL
        GROUP BY m.work_id
        LIMIT 60
      ),
      evaluated_works AS (
        SELECT 
          ss.work_id,
          ss.frontier_sort_key,
          ss.total_staged_chapters,
          EXISTS (
            SELECT 1 
            FROM importer_chapter_mappings pm
            WHERE pm.work_id = ss.work_id
              AND pm.chapter_sort_key < ss.frontier_sort_key
              AND pm.is_gap = false
              AND NOT EXISTS (
                SELECT 1 FROM chapters c 
                WHERE c.work_id = ss.work_id 
                  AND c.number = pm.chapter_sort_key 
                  AND c.published_at IS NOT NULL
              )
          ) as has_unpub_mapping_predecessor,
          EXISTS (
            SELECT 1 
            FROM importer_queue pq
            WHERE (pq.payload->>'workId') = ss.work_id::text
              AND pq.task_type = 'IMPORT_CHAPTER'
              AND pq.status IN ('QUEUED', 'RETRY', 'IMPORTING')
              AND pq.chapter_sort_key < ss.frontier_sort_key
          ) as has_active_queue_predecessor,
          EXISTS (
            SELECT 1 
            FROM importer_queue sq
            WHERE sq.task_type = 'SYNC_WORK'
              AND sq.status IN ('QUEUED', 'IMPORTING')
              AND (sq.payload->>'workId') = ss.work_id::text
          ) as has_sync_in_progress,
          EXISTS (
            SELECT 1 
            FROM importer_queue aq
            WHERE (aq.payload->>'workId') = ss.work_id::text
              AND aq.status IN ('QUEUED', 'RETRY', 'IMPORTING')
          ) as has_any_active_work_in_queue
        FROM staged_summary ss
      )
      SELECT 
        work_id,
        frontier_sort_key,
        total_staged_chapters,
        CASE 
          WHEN NOT has_unpub_mapping_predecessor 
           AND NOT has_active_queue_predecessor 
           AND NOT has_sync_in_progress 
          THEN 1 
          ELSE 0 
        END as publishable_count,
        CASE 
          WHEN NOT has_unpub_mapping_predecessor 
           AND NOT has_active_queue_predecessor 
           AND NOT has_sync_in_progress 
          THEN total_staged_chapters - 1
          ELSE total_staged_chapters
        END as non_publishable_count,
        CASE 
          WHEN has_unpub_mapping_predecessor OR has_active_queue_predecessor OR has_sync_in_progress THEN
            CASE WHEN has_any_active_work_in_queue OR has_sync_in_progress THEN 'WAITING_PREDECESSOR' ELSE 'STUCK' END
          ELSE 'PUBLISHABLE'
        END as work_staged_state
      FROM evaluated_works;
    `;
    const res = await client.query(q);
    console.log("Evaluated works rows count:", res.rows.length);
    const summary = { publishable: 0, waiting: 0, stuck: 0 };
    for (const r of res.rows) {
      if (r.work_staged_state === 'PUBLISHABLE') {
        summary.publishable += parseInt(r.publishable_count);
        summary.waiting += parseInt(r.non_publishable_count);
      } else if (r.work_staged_state === 'WAITING_PREDECESSOR') {
        summary.waiting += parseInt(r.non_publishable_count);
      } else {
        summary.stuck += parseInt(r.non_publishable_count);
      }
    }
    console.log("Summary:", summary);
    console.log("Sample rows:", res.rows.slice(0, 5));
  } finally {
    client.release();
    await pool.end();
  }
}
run().catch(console.error);
