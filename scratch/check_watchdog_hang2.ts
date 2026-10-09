import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const SOURCE_EXECUTION_ELIGIBILITY_SQL = `(
  (
    s.status = 'ACTIVE'
    AND (
      s.blocked_reason IS NULL
      OR s.blocked_details->>'probe_success' = 'true'
      OR s.blocked_details->>'recovered_at' IS NOT NULL
    )
  )
  OR (
    s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED')
    AND (
      s.blocked_reason IS NULL
      OR s.blocked_details->>'probe_success' = 'true'
      OR s.blocked_details->>'recovered_at' IS NOT NULL
    )
    AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())
  )
)`;

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
  console.log("Running slow watchdog query...");
  try {
    const res = await pool.query(`
      SELECT count(*) FROM (
        WITH candidate_works AS MATERIALIZED (
          SELECT 
            q.payload->>'workId' AS work_id,
            MIN(q.chapter_sort_key) as min_chapter_sort_key
          FROM importer_queue q
          JOIN importer_sources s ON q.source = s.id
          JOIN works w ON w.id = (q.payload->>'workId')::uuid
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
            AND q.priority >= 50
            AND s.enabled = true
            AND w.published IS TRUE
            AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          GROUP BY q.payload->>'workId'
          ORDER BY MIN(q.chapter_sort_key) ASC NULLS LAST
          LIMIT 100
        )
        SELECT 1
        FROM candidate_works cw
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
      ) as eligible_cnt
    `);
    console.log("Success:", res.rows, "Time:", Date.now() - t0);
  } catch (err) {
    console.error("Error:", err.message);
  }
  await pool.end();
}
run();
