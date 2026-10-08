import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import { CANONICAL_FRONTIER_CLAIM_FILTER, CANONICAL_PUBLISHED_CLAIM_FILTER, CANONICAL_ACTIVE_CLAIM_FILTER } from '../src/core/scheduler/work-affinity-scheduler.js';
import { SOURCE_EXECUTION_ELIGIBILITY_SQL } from '../src/core/source-eligibility.js';

dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function run() {
  const qRes = await pool.query(`
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
        ) as sub) as eligible_cnt_new,
        
        (SELECT count(*) FROM (
          WITH candidate_works AS MATERIALIZED (
            SELECT 
              (q.payload->>'workId') AS work_id,
              MIN(q.chapter_sort_key) as min_chapter_sort_key
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
            GROUP BY q.payload->>'workId'
          )
          SELECT 1
          FROM candidate_works cw
          JOIN works w ON w.id = cw.work_id::uuid
          CROSS JOIN LATERAL (
            SELECT MAX(c.number) AS max_published
            FROM chapters c
            WHERE c.work_id = cw.work_id::uuid
              AND c.published_at IS NOT NULL
          ) pub
          WHERE w.published IS TRUE
            AND (
              (pub.max_published IS NULL AND cw.min_chapter_sort_key = 0) OR
              (pub.max_published IS NOT NULL AND cw.min_chapter_sort_key <= pub.max_published + 1)
            )
        ) sub) as eligible_cnt_old
  `);
  console.log(qRes.rows[0]);
  pool.end();
}
run();
