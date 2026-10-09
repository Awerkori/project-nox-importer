import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

import { CANONICAL_PUBLISHED_CLAIM_FILTER, CANONICAL_FRONTIER_CLAIM_FILTER, CANONICAL_ACTIVE_CLAIM_FILTER } from '../src/core/scheduler/work-affinity-scheduler.js';
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

  try {
    const q = `
      WITH dummy_params AS (SELECT $1::text[], $2::text[], $3::text[], $4::text, $5::int, $6::int, $7::int),
      eligible_sources AS MATERIALIZED (
        SELECT s.id
        FROM importer_sources s
        WHERE s.enabled = true
          AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
        ORDER BY s.id ASC
        LIMIT $7
      ),
      source_window AS MATERIALIZED (
        SELECT candidate.id
        FROM eligible_sources s
        CROSS JOIN LATERAL (
          SELECT q.id
          FROM importer_queue q
          WHERE q.source = s.id
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
            AND q.task_type = 'IMPORT_CHAPTER'
            AND q.attempts < COALESCE(q.max_attempts, 7)
          -- Uses idx_importer_queue_fetch; canonical selection remains below.
          ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
          LIMIT $6
        ) candidate
      ),
      to_lock_filtered AS (
        SELECT q.id, q.payload, q.status, q.task_type, q.attempts, q.max_attempts, q.next_run_at, q.priority, q.chapter_sort_key, q.source
        FROM source_window windowed
        JOIN importer_queue q ON q.id = windowed.id
        JOIN works w ON w.id = (q.payload->>'workId')::uuid
        WHERE w.published = true
      ),
      to_lock AS (
        SELECT q_base.id
        FROM to_lock_filtered q
        JOIN importer_queue q_base ON q_base.id = q.id
        JOIN importer_sources s ON s.id = q.source
        LEFT JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = (q.payload->>'workId')::uuid
            AND c.published_at IS NOT NULL
        ) pub ON TRUE
        WHERE (
          q.status = 'QUEUED'
          OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
        )
          AND q.task_type = 'IMPORT_CHAPTER'
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND s.enabled = true
          AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          ${CANONICAL_PUBLISHED_CLAIM_FILTER}
          ${CANONICAL_FRONTIER_CLAIM_FILTER}
          ${CANONICAL_ACTIVE_CLAIM_FILTER}
        ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
        FOR UPDATE OF q_base SKIP LOCKED
        LIMIT 1
      )
      UPDATE importer_queue q
      SET status = 'IMPORTING'
      FROM to_lock
      WHERE q.id = to_lock.id
      RETURNING q.id;
    `;
    const res = await pool.query("EXPLAIN ANALYZE " + q, [
        null, null, null, 'worker', 5, 20, 2
    ]);
    console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
run();
