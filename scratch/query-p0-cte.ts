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

  const SOURCE_EXECUTION_ELIGIBILITY_SQL = `
      s.enabled = true
      AND (
        (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
        OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
      )
  `;

  const CANONICAL_PUBLISHED_CLAIM_FILTER = `
      AND NOT EXISTS (
        SELECT 1
        FROM chapters canonical_chapter
        WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
          AND canonical_chapter.published_at IS NOT NULL
          AND canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
      )
  `;

  const CANONICAL_FRONTIER_CLAIM_FILTER = `
      AND NOT EXISTS (
        SELECT 1
        FROM importer_queue predecessor
        WHERE predecessor.task_type = 'IMPORT_CHAPTER'
          AND predecessor.payload->>'workId' = q.payload->>'workId'
          AND predecessor.chapter_sort_key < q.chapter_sort_key
          AND predecessor.status IN ('QUEUED', 'RETRY', 'IMPORTING')
          AND NOT EXISTS (
            SELECT 1
            FROM chapters predecessor_canonical
            WHERE predecessor_canonical.work_id = (predecessor.payload->>'workId')::uuid
              AND predecessor_canonical.published_at IS NOT NULL
              AND predecessor_canonical.number = COALESCE(NULLIF(predecessor.payload->>'chapterNumber', '')::numeric, predecessor.chapter_sort_key)
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM importer_chapter_mappings staged_frontier
        WHERE staged_frontier.work_id = (q.payload->>'workId')::uuid
          AND staged_frontier.chapter_sort_key = q.chapter_sort_key
          AND staged_frontier.status IN ('STAGED', 'WAITING_FOR_GAP')
      )
  `;

  const CANONICAL_ACTIVE_CLAIM_FILTER = `
      AND NOT EXISTS (
        SELECT 1 FROM importer_queue in_flight
        WHERE in_flight.task_type = 'IMPORT_CHAPTER'
          AND in_flight.status = 'IMPORTING'
          AND in_flight.payload->>'workId' = q.payload->>'workId'
          AND in_flight.chapter_sort_key <= q.chapter_sort_key
      )
  `;

  const q = `
      WITH candidate_works AS MATERIALIZED (
        SELECT 
          q.payload->>'workId' AS work_id,
          MIN(q.chapter_sort_key) as min_chapter_sort_key,
          MIN(q.next_run_at) as min_next_run_at
        FROM importer_queue q
        JOIN importer_sources s ON s.id = q.source
        WHERE q.task_type = 'IMPORT_CHAPTER'
          AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND q.priority >= 100 AND q.priority < 1000
          AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
        GROUP BY q.payload->>'workId'
        ORDER BY MIN(q.next_run_at) ASC, MIN(q.chapter_sort_key) ASC NULLS LAST
        LIMIT 100
      )
      SELECT q.payload->>'workId' AS work_id
      FROM candidate_works cw
      JOIN importer_queue q 
        ON (q.payload->>'workId') = cw.work_id 
        AND q.chapter_sort_key = cw.min_chapter_sort_key
      LEFT JOIN LATERAL (
        SELECT MAX(c.number) AS max_published
        FROM chapters c
        WHERE c.work_id = (q.payload->>'workId')::uuid
          AND c.published_at IS NOT NULL
      ) pub ON TRUE
      WHERE q.task_type = 'IMPORT_CHAPTER'
        AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
        ${CANONICAL_PUBLISHED_CLAIM_FILTER}
        ${CANONICAL_FRONTIER_CLAIM_FILTER}
        ${CANONICAL_ACTIVE_CLAIM_FILTER}
      GROUP BY q.payload->>'workId'
      ORDER BY MIN(cw.min_next_run_at) ASC, MIN(cw.min_chapter_sort_key) ASC NULLS LAST
      LIMIT 32;
  `;
  const t0 = Date.now();
  const res = await pool.query(q);
  console.log("P0 Scan Candidates:", res.rows);
  console.log("Time (ms):", Date.now() - t0);
  await pool.end();
}
run().catch(console.error);
