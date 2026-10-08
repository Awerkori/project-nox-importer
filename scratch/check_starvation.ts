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


async function main() {
  try {
    const res = await pool.query(`
      WITH source_window AS MATERIALIZED (
        SELECT q.id
        FROM importer_queue q
        WHERE q.source = 'montetai'
          AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
          AND q.task_type = 'IMPORT_CHAPTER'
        ORDER BY q.created_at ASC NULLS LAST, q.id ASC
        LIMIT 50
      )
      SELECT COUNT(*) as claimable_in_window
      FROM source_window w
      JOIN importer_queue q ON q.id = w.id
      WHERE 1=1 
        ${CANONICAL_PUBLISHED_CLAIM_FILTER}
        ${CANONICAL_FRONTIER_CLAIM_FILTER}
    `);
    console.log("Claimable in oldest 50 montetai jobs:", res.rows[0].claimable_in_window);
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
main();
