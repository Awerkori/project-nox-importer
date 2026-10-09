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
      WITH dummy_params AS (SELECT $1::text[], $2::int, $3::text, $4::numeric, $5::text, $6::int, $7::text[], $8::text[], $9::text[], $10::int),
      q_candidates AS (
        SELECT id, payload, source, chapter_sort_key, next_run_at, priority
        FROM importer_queue
        WHERE (status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW()))
          AND task_type = 'IMPORT_CHAPTER'
          AND attempts < COALESCE(max_attempts, 7)
      ),
      to_lock AS (
        SELECT q_base.id
        FROM importer_queue q_base
        JOIN q_candidates q ON q_base.id = q.id
        JOIN importer_sources s ON s.id = q.source
        LEFT JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = (q.payload->>'workId')::uuid
            AND c.published_at IS NOT NULL
        ) pub ON TRUE
        WHERE s.enabled = true
          AND (s.status = 'ACTIVE' OR s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED'))
          AND NOT EXISTS (
            SELECT 1
            FROM chapters canonical_chapter
            WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
              AND canonical_chapter.published_at IS NOT NULL
              AND (
                canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
              )
          )
        ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
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
        null, null, null, null, 'worker', 5, null, null, null, null
    ]);
    console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
run();
