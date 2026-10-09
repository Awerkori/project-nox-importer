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
          WITH candidate_works AS MATERIALIZED (
            SELECT 
              q.payload->>'workId' AS work_id,
              MIN(q.chapter_sort_key) as min_chapter_sort_key
            FROM importer_queue q
            JOIN importer_sources s ON q.source = s.id
            WHERE q.task_type = 'IMPORT_CHAPTER'
              AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
              AND q.priority >= 50
              AND s.enabled = true
            GROUP BY q.payload->>'workId'
            ORDER BY MIN(q.chapter_sort_key) ASC NULLS LAST
            LIMIT 100
          )
          SELECT cw.work_id, cw.min_chapter_sort_key, pub.max_published
          FROM candidate_works cw 
          JOIN works w ON w.id = cw.work_id::uuid
          LEFT JOIN LATERAL (
            SELECT MAX(c.number) AS max_published
            FROM chapters c
            WHERE c.work_id = cw.work_id::uuid
              AND c.published_at IS NOT NULL
          ) pub ON TRUE
          WHERE w.published IS TRUE
  `);
  console.log("Details for the published works:");
  console.log(res.rows);
  await pool.end();
}
run();
