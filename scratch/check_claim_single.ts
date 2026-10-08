import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

const SOURCE_EXECUTION_ELIGIBILITY_SQL = `
              (
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
              AND (
                canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
              )
          )`;
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
                WHERE predecessor_canonical.work_id = (q.payload->>'workId')::uuid
                  AND predecessor_canonical.published_at IS NOT NULL
                  AND (
                    predecessor_canonical.number = NULLIF(predecessor.payload->>'chapterNumber', '')::numeric
                    OR predecessor_canonical.number = predecessor.chapter_sort_key
                  )
              )
          )
          AND NOT EXISTS (
            SELECT 1
            FROM importer_chapter_mappings staged_frontier
            WHERE staged_frontier.work_id = (q.payload->>'workId')::uuid
              AND staged_frontier.chapter_sort_key = q.chapter_sort_key
              AND staged_frontier.status IN ('STAGED', 'WAITING_FOR_GAP')
          )`;

const CANONICAL_ACTIVE_CLAIM_FILTER = `
          AND NOT EXISTS (
            SELECT 1
            FROM importer_queue active_chapter
            WHERE active_chapter.task_type = 'IMPORT_CHAPTER'
              AND active_chapter.status = 'IMPORTING'
              AND (active_chapter.payload->>'workId') = (q.payload->>'workId')
              AND active_chapter.chapter_sort_key = q.chapter_sort_key
          )`;

async function main() {
  const query = `
      WITH q_candidates AS (
        SELECT id, payload, source, chapter_sort_key, next_run_at, priority
        FROM importer_queue
        WHERE (status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW()))
          AND task_type = 'IMPORT_CHAPTER'
          AND attempts < COALESCE(max_attempts, 7)
          AND priority >= 100
          AND priority <= 999
          AND (payload->>'workId') = '90ea547a-f7c5-4d63-9dfb-10cc480c3fdc'
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
          AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          ${CANONICAL_PUBLISHED_CLAIM_FILTER}
          ${CANONICAL_FRONTIER_CLAIM_FILTER}
          ${CANONICAL_ACTIVE_CLAIM_FILTER}
        ORDER BY q.chapter_sort_key ASC NULLS LAST
        LIMIT 1
      )
      SELECT * FROM to_lock;
  `;
  try {
    const res = await pool.query(query);
    console.log("claimSingleJob returns:", res.rows);
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
main();
