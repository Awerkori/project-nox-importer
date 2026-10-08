import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import fs from 'fs';
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

const CANONICAL_PUBLISHED_CLAIM_FILTER = `
  AND (
    q.chapter_sort_key > pub.max_published 
    OR pub.max_published IS NULL
  )
`;

const CANONICAL_FRONTIER_CLAIM_FILTER = `
  AND NOT EXISTS (
    SELECT 1 
    FROM importer_queue p
    WHERE p.payload->>'workId' = q.payload->>'workId'
      AND p.chapter_sort_key < q.chapter_sort_key
      AND p.status IN ('QUEUED', 'RETRY', 'IMPORTING', 'BLOCKED_BY_UPSTREAM')
  )
`;

const CANONICAL_ACTIVE_CLAIM_FILTER = `
  AND NOT EXISTS (
    SELECT 1 
    FROM importer_queue a
    WHERE a.payload->>'workId' = q.payload->>'workId'
      AND a.chapter_sort_key = q.chapter_sort_key
      AND a.status = 'IMPORTING'
      AND a.id != q.id
  )
`;

const query = `
  PREPARE test_plan(text[], int, text, numeric, text, int, text[], text[], text[], int) AS
  WITH q_candidates AS (
    SELECT id, payload, source, chapter_sort_key, next_run_at, priority
    FROM importer_queue
    WHERE (status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW()))
      AND task_type = 'IMPORT_CHAPTER'
      AND attempts < COALESCE(max_attempts, 7)
      AND ($2::int IS NULL OR priority >= $2::int)
      AND ($10::int IS NULL OR priority <= $10::int)
      AND ($3::text IS NULL OR (payload->>'workId') = $3::text)
      AND ($4::numeric IS NULL OR chapter_sort_key = $4::numeric)
      AND ($7::text[] IS NULL OR (payload->>'workId') = ANY($7::text[]))
  )
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
    AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
    ${CANONICAL_PUBLISHED_CLAIM_FILTER}
    ${CANONICAL_FRONTIER_CLAIM_FILTER}
    AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
    AND ($8::text[] IS NULL OR NOT ((q.payload->>'workId') = ANY($8::text[])))
    AND ($9::text[] IS NULL OR NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($9::text[])))
    ${CANONICAL_ACTIVE_CLAIM_FILTER}
  ORDER BY
    q.priority DESC, 
    q.chapter_sort_key ASC NULLS LAST, 
    q.next_run_at ASC
  LIMIT 1;
`;

async function run() {
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  await pool.query(query);
  const res = await pool.query(`EXPLAIN ANALYZE EXECUTE test_plan(
    NULL, NULL, NULL, NULL, 'worker-1', 5,
    ARRAY['e8bf25c8-470b-414a-b606-89f704e25919', 'c3017950-fa64-4c31-9eaf-8c6aaf63440b'],
    NULL, NULL, NULL
  )`);
  const out = res.rows.map(r => r['QUERY PLAN']).join('\n');
  fs.writeFileSync('scratch/explain_claim3.out', out);
  console.log("Done. Check scratch/explain_claim3.out");
  await pool.end();
}
run().catch(console.error);
