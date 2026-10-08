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
          WITH candidate_works AS MATERIALIZED (
            SELECT 
              (q.payload->>'workId') AS work_id,
              MIN(q.chapter_sort_key) as min_chapter_sort_key,
              MIN(q.next_run_at) as min_next_run_at
            FROM importer_queue q
            LEFT JOIN importer_sources s ON q.source = s.id
            WHERE q.status IN ('QUEUED', 'RETRY')
              AND q.task_type = 'IMPORT_CHAPTER'
              AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
              AND q.priority >= 50
              AND s.enabled = true
              AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
            GROUP BY q.payload->>'workId'
            ORDER BY MIN(q.chapter_sort_key) ASC NULLS LAST
            LIMIT 100
          )
          SELECT cw.work_id
          FROM candidate_works cw
          JOIN works w ON w.id = cw.work_id::uuid
          CROSS JOIN LATERAL (
            SELECT MAX(c.number) AS max_published
            FROM chapters c
            WHERE c.work_id = cw.work_id::uuid
              AND c.published_at IS NOT NULL
          ) pub
          JOIN importer_queue q ON q.payload->>'workId' = cw.work_id
          WHERE w.published IS TRUE
            AND (
              (pub.max_published IS NULL AND cw.min_chapter_sort_key = 0) OR
              (pub.max_published IS NOT NULL AND cw.min_chapter_sort_key <= pub.max_published + 1)
            )
            AND q.task_type = 'IMPORT_CHAPTER'
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
            ${CANONICAL_PUBLISHED_CLAIM_FILTER}
            ${CANONICAL_FRONTIER_CLAIM_FILTER}
            ${CANONICAL_ACTIVE_CLAIM_FILTER}
          GROUP BY q.payload->>'workId'
          ORDER BY MIN(cw.min_next_run_at) ASC, MIN(cw.min_chapter_sort_key) ASC NULLS LAST
          LIMIT 32;
  `;
  try {
    const res = await pool.query(query);
    console.log("P0 candidates:", res.rows);
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
main();
