import pg from 'pg';
import { CANONICAL_FRONTIER_CLAIM_FILTER, CANONICAL_PUBLISHED_CLAIM_FILTER, CANONICAL_ACTIVE_CLAIM_FILTER } from './src/core/scheduler/work-affinity-scheduler.js';
import { SOURCE_EXECUTION_ELIGIBILITY_SQL } from './src/core/source-eligibility.js';

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/nox'
});

async function run() {
  const qRes = await pool.query(`
      SELECT 
        (SELECT COUNT(DISTINCT cw.work_id) FROM (
          WITH candidate_works AS MATERIALIZED (
            SELECT 
              (q.payload->>'workId') AS work_id,
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
          SELECT cw.work_id
          FROM candidate_works cw
          JOIN works w ON w.id = cw.work_id::uuid
          JOIN importer_queue q
            ON (q.payload->>'workId') = cw.work_id
            AND q.chapter_sort_key = cw.min_chapter_sort_key
          LEFT JOIN LATERAL (
            SELECT MAX(c.number) AS max_published
            FROM chapters c
            WHERE c.work_id = (q.payload->>'workId')::uuid
              AND c.published_at IS NOT NULL
          ) pub ON TRUE
          WHERE w.published IS TRUE
            AND q.task_type = 'IMPORT_CHAPTER'
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
            ${CANONICAL_PUBLISHED_CLAIM_FILTER}
            ${CANONICAL_FRONTIER_CLAIM_FILTER}
            ${CANONICAL_ACTIVE_CLAIM_FILTER}
        ) as sub) as eligible_cnt_new
  `);
  console.log(qRes.rows[0]);
  pool.end();
}
run();
