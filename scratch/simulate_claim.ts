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
          ORDER BY q.created_at ASC NULLS LAST, q.id ASC
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
          AND w.published = true
          AND s.enabled = true
          AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          ${CANONICAL_PUBLISHED_CLAIM_FILTER}
          ${CANONICAL_FRONTIER_CLAIM_FILTER}
          ${CANONICAL_ACTIVE_CLAIM_FILTER}
        ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
        LIMIT 1
      )
      SELECT * FROM to_lock;
  `;
  try {
    const res = await pool.query(query, [null, null, null, 'worker-1', 15, 32, 20]);
    console.log("Simulated claim:", res.rows);
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
main();
