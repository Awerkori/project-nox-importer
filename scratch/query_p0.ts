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
              AND s.enabled = true
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
  const res = await pool.query(query);
  const t1 = Date.now();
  console.log(`P0 works: ${res.rows.length} in ${t1-t0}ms`);
  
  // What does candidate_works alone return?
  const cw = await pool.query(`
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
      AND s.enabled = true
      AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
    GROUP BY q.payload->>'workId'
    ORDER BY MIN(q.next_run_at) ASC, MIN(q.chapter_sort_key) ASC NULLS LAST
    LIMIT 100
  `);
  console.log(`candidate_works alone: ${cw.rows.length}`);
  
  // Check if they are published
  if (cw.rows.length > 0) {
    const ids = cw.rows.map(r => `'${r.work_id}'`).join(',');
    const pub = await pool.query(`SELECT id, published FROM works WHERE id IN (${ids})`);
    console.table(pub.rows);
  }

  pool.end();
}
main().catch(console.error);
