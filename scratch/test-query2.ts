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
      WITH dummy_params AS (SELECT $1::text[], $2::text[], $3::text[], $4::text, $5::int, $6::text[]),
      q_candidates AS (
        SELECT id, payload, source, chapter_sort_key, next_run_at, priority, status, task_type, attempts, max_attempts
        FROM importer_queue
        WHERE (status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW()))
          AND task_type = 'IMPORT_CHAPTER'
          AND attempts < COALESCE(max_attempts, 7)
          AND (
            priority >= 1000
          )
      ),
      to_lock AS (
        SELECT q_base.id
        FROM q_candidates q
        JOIN importer_queue q_base ON q_base.id = q.id
        JOIN importer_sources s ON s.id = q.source
        LEFT JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = (q.payload->>'workId')::uuid
            AND c.published_at IS NOT NULL
        ) pub ON TRUE
        LEFT JOIN importer_staff_requests sr
          ON sr.work_id = (q.payload->>'workId')::uuid
         AND sr.status IN ('ACTIVE', 'QUEUED', 'IMPORTING', 'RETRYING')
        WHERE s.enabled = true
          AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          ${CANONICAL_PUBLISHED_CLAIM_FILTER}
          ${CANONICAL_FRONTIER_CLAIM_FILTER}
          ${CANONICAL_ACTIVE_CLAIM_FILTER}
        ORDER BY
          CASE WHEN sr.work_id IS NOT NULL THEN 0 ELSE 1 END,
          COALESCE(array_position($6::text[], q.payload->>'workId'), 2147483647),
          q.priority DESC,
          q.chapter_sort_key ASC NULLS LAST,
          q.next_run_at ASC
        FOR UPDATE OF q_base SKIP LOCKED
        LIMIT 1
      )
      UPDATE importer_queue q_base
      SET status = 'IMPORTING'
      FROM to_lock
      WHERE q_base.id = to_lock.id
      RETURNING q_base.id;
    `;
    const res = await pool.query("EXPLAIN ANALYZE " + q, [
        null, null, null, 'worker', 5, []
    ]);
    console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
run();
