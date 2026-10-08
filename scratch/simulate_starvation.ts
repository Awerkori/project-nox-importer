import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
});

async function run() {
  const res = await pool.query(`
      WITH eligible_sources AS MATERIALIZED (
        SELECT s.id
        FROM importer_sources s
        WHERE s.enabled = true
        ORDER BY s.id ASC
        LIMIT 20
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
          LIMIT 50
        ) candidate
      )
      SELECT count(*) FROM source_window;
  `);
  console.log("Total in window:", res.rows[0].count);
  
  const toLock = await pool.query(`
      WITH eligible_sources AS MATERIALIZED (
        SELECT s.id
        FROM importer_sources s
        WHERE s.enabled = true
        ORDER BY s.id ASC
        LIMIT 20
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
          LIMIT 50
        ) candidate
      )
      SELECT q.id
      FROM source_window windowed
      JOIN importer_queue q ON q.id = windowed.id
      JOIN works w ON w.id = (q.payload->>'workId')::uuid
      LEFT JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = (q.payload->>'workId')::uuid
            AND c.published_at IS NOT NULL
        ) pub ON TRUE
      WHERE 
          NOT EXISTS (
            SELECT 1
            FROM chapters canonical_chapter
            WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
              AND canonical_chapter.number = q.chapter_sort_key
              AND canonical_chapter.published_at IS NOT NULL
          )
          AND NOT EXISTS (
            SELECT 1
            FROM importer_queue predecessor
            WHERE predecessor.payload->>'workId' = q.payload->>'workId'
              AND predecessor.chapter_sort_key < q.chapter_sort_key
              AND predecessor.task_type = 'IMPORT_CHAPTER'
              AND predecessor.status IN ('QUEUED', 'RETRY', 'STAGED', 'IMPORTING')
              AND (
                pub.max_published IS NULL 
                OR predecessor.chapter_sort_key > pub.max_published
              )
          )
  `);
  console.log("Claimable from window:", toLock.rows.length);
  pool.end();
}
run();
