import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
});

async function main() {
  const SOURCE_EXECUTION_ELIGIBILITY_SQL = `(
    (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
    OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
  )`;

  const CANONICAL_PUBLISHED_CLAIM_FILTER = `AND q.chapter_sort_key > COALESCE(pub.max_published, -1)`;
  const CANONICAL_FRONTIER_CLAIM_FILTER = `AND q.chapter_sort_key <= COALESCE(pub.max_published, -1) + 8`;
  const CANONICAL_ACTIVE_CLAIM_FILTER = `
    AND NOT EXISTS (
      SELECT 1 FROM importer_queue conflict
      WHERE conflict.task_type = 'IMPORT_CHAPTER'
        AND conflict.status IN ('IMPORTING', 'ACTIVE', 'RETRYING')
        AND conflict.payload->>'workId' = q.payload->>'workId'
        AND conflict.chapter_sort_key < q.chapter_sort_key
    )
  `;

  const query = `
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
          JOIN works w ON w.id = (q.payload->>'workId')::uuid
          WHERE q.source = s.id
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
            AND q.task_type = 'IMPORT_CHAPTER'
            AND q.attempts < COALESCE(q.max_attempts, 7)
            AND w.published = true
          ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
          LIMIT $6
        ) candidate
      ),
      to_lock AS (
        SELECT q.id
        FROM source_window windowed
        JOIN importer_queue q ON q.id = windowed.id
        JOIN works w ON w.id = (q.payload->>'workId')::uuid
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
        FOR UPDATE OF q SKIP LOCKED
        LIMIT $5
      )
      SELECT q.id, q.payload, q.source, q.priority, q.chapter_sort_key
      FROM to_lock
      JOIN importer_queue q ON q.id = to_lock.id;
  `;
  const params = [ [], [], [], 'manual-test-worker', 5, 100, 10 ];
  const t0 = Date.now();
  const res = await pool.query(query, params);
  const t1 = Date.now();
  console.log(`Acquired ${res.rows.length} jobs in ${t1-t0}ms`);
  console.table(res.rows.map(r => ({ id: r.id, workId: r.payload.workId, sortKey: r.chapter_sort_key })));
  
  pool.end();
}
main().catch(console.error);
