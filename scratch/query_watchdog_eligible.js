import pg from 'pg';
import { CANONICAL_FRONTIER_CLAIM_FILTER } from './src/core/scheduler/work-affinity-scheduler.js';

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/nox'
});

async function run() {
  const qRes = await pool.query(`
      SELECT 
        (SELECT COUNT(DISTINCT q.id) FROM importer_queue q
            LEFT JOIN importer_sources s ON q.source = s.id
            JOIN works w ON w.id = (q.payload->>'workId')::uuid
            CROSS JOIN LATERAL (
              SELECT MAX(c.number) AS max_published
              FROM chapters c
              WHERE c.work_id = w.id
                AND c.published_at IS NOT NULL
            ) pub
            WHERE q.status IN ('QUEUED', 'RETRY')
              AND q.task_type = 'IMPORT_CHAPTER'
              AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
              AND q.priority >= 50
              AND w.published IS TRUE
              AND s.enabled = true
              AND (
                (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
                OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
              )
              ${CANONICAL_FRONTIER_CLAIM_FILTER}
        ) as eligible_cnt_new,
        (SELECT COUNT(*) FROM (
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
