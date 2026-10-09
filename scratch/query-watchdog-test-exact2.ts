import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();
import { CANONICAL_FRONTIER_CLAIM_FILTER, CANONICAL_PUBLISHED_CLAIM_FILTER } from '../src/core/scheduler/work-affinity-scheduler.js';
import { SOURCE_EXECUTION_ELIGIBILITY_SQL } from '../src/core/source-eligibility.js';

async function run() {
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  const sql = `
      SELECT 
        (SELECT count(*) FROM (
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
              AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
            GROUP BY q.payload->>'workId'
            ORDER BY MIN(q.chapter_sort_key) ASC NULLS LAST
            LIMIT 100
          )
          SELECT 1
          FROM candidate_works cw
          JOIN works w ON w.id = cw.work_id::uuid
          JOIN importer_queue q
            ON (q.payload->>'workId') = cw.work_id
            AND q.chapter_sort_key = cw.min_chapter_sort_key
            AND q.task_type = 'IMPORT_CHAPTER'
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
          LEFT JOIN LATERAL (
            SELECT MAX(c.number) AS max_published
            FROM chapters c
            WHERE c.work_id = (q.payload->>'workId')::uuid
              AND c.published_at IS NOT NULL
          ) pub ON TRUE
          WHERE w.published IS TRUE
            ${CANONICAL_PUBLISHED_CLAIM_FILTER}
            ${CANONICAL_FRONTIER_CLAIM_FILTER}
        ) sub) as eligible_cnt,
        (SELECT count(*) FROM importer_queue WHERE status = 'IMPORTING') as importing_cnt,
        (SELECT count(*) FROM importer_queue WHERE status = 'RETRY') as retry_cnt
    `;
    
  try {
    const res = await pool.query(sql);
    console.log("Success:", res.rows[0]);
  } catch (err) {
    console.error("SQL Error:", err.message);
  } finally {
    await pool.end();
  }
}
run();
