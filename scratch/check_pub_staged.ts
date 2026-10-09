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

  const t0 = Date.now();
  console.log("Running pubStagedRes query...");
  try {
    const pubStagedRes = await pool.query(`
          WITH staged_works AS (
            SELECT 
              m.work_id,
              MIN(m.chapter_sort_key) as frontier_sort_key,
              COUNT(DISTINCT m.chapter_sort_key) as total_staged_chapters
            FROM importer_chapter_mappings m
            WHERE m.status IN ('STAGED', 'WAITING_FOR_GAP') AND m.work_id IS NOT NULL
            GROUP BY m.work_id
            HAVING ($1::numeric IS NULL OR (MIN(m.chapter_sort_key) > $1::numeric OR (MIN(m.chapter_sort_key) = $1::numeric AND m.work_id::text > $2::text)))
            ORDER BY MIN(m.chapter_sort_key) ASC, m.work_id ASC
            LIMIT 40
          ),
          works_with_published AS (
            SELECT 
              sw.work_id,
              sw.frontier_sort_key,
              sw.total_staged_chapters,
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
              ) as has_predecessor_in_queue,
              EXISTS (
                SELECT 1
                FROM importer_chapter_mappings pg
                WHERE pg.work_id = sw.work_id
                  AND pg.chapter_sort_key < sw.frontier_sort_key
                  AND pg.status = 'WAITING_FOR_GAP'
              ) as has_predecessor_waiting_for_gap
            FROM staged_works sw
          )
          SELECT 
            work_id,
            frontier_sort_key,
            total_staged_chapters,
            max_published,
            has_predecessor_in_mapping,
            has_predecessor_in_queue,
            CASE
              WHEN (max_published IS NOT NULL AND frontier_sort_key <= max_published + 1.05 AND NOT has_predecessor_in_queue)
                OR (max_published IS NULL AND NOT has_predecessor_in_mapping AND NOT has_predecessor_in_queue)
              THEN 1
              ELSE 0
            END as is_frontier_publishable
          FROM works_with_published;
    `, [null, null]);
    console.log("Success:", pubStagedRes.rows.length, "rows. Time:", Date.now() - t0);
  } catch (err) {
    console.error("Error:", err.message);
  }
  await pool.end();
}
run();
