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
      SELECT 
        (SELECT COUNT(*) FROM (
          WITH candidate_works AS MATERIALIZED (
            SELECT 
              (q.payload->>'workId') AS work_id,
              q.chapter_sort_key
            FROM importer_queue q
            LEFT JOIN importer_sources s ON q.source = s.id
            WHERE q.status IN ('QUEUED', 'RETRY')
              AND q.task_type = 'IMPORT_CHAPTER'
              AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
              AND q.priority >= 50
              AND s.enabled = true
              AND (
                (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
                OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
              )
            LIMIT 2000
          )
          SELECT 1
          FROM candidate_works cw
          JOIN works w ON w.id = cw.work_id::uuid
          WHERE w.published IS TRUE
            AND cw.chapter_sort_key <= COALESCE((
              SELECT MAX(c.number) 
              FROM chapters c 
              WHERE c.work_id = cw.work_id::uuid 
                AND c.published_at IS NOT NULL
            ), -1) + 1
          LIMIT 500
        ) sub) as eligible_cnt
  `);
  console.log("Eligible count CTE:", res.rows[0].eligible_cnt);
  await pool.end();
}
run();
