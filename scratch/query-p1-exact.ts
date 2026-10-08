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
            AND (
              (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
              OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
            )
          GROUP BY q.payload->>'workId'
          ORDER BY MIN(q.next_run_at) ASC, MIN(q.chapter_sort_key) ASC NULLS LAST
          LIMIT 100
        )
        SELECT q.status, cw.work_id
        FROM candidate_works cw
        JOIN importer_queue q 
          ON (q.payload->>'workId') = cw.work_id 
          AND q.chapter_sort_key = cw.min_chapter_sort_key
        JOIN works w ON w.id = cw.work_id::uuid
        CROSS JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = cw.work_id::uuid
            AND c.published_at IS NOT NULL
        ) pub
        WHERE q.task_type = 'IMPORT_CHAPTER'
          AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
          AND w.published IS TRUE
          AND (
            (pub.max_published IS NULL AND cw.min_chapter_sort_key = 0) OR
            (pub.max_published IS NOT NULL AND cw.min_chapter_sort_key <= pub.max_published + 1)
          )
        LIMIT 100
  `);
  console.log("P1 Ready candidate works:", res.rows);
  await pool.end();
}
run();
