import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

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
          )
          AND (
            (pub.max_published IS NOT NULL AND q.chapter_sort_key <= pub.max_published + 1.5)
            OR EXISTS (
              SELECT 1
              FROM importer_confirmed_gaps gap
              WHERE gap.work_id = (q.payload->>'workId')::uuid
                AND gap.start_sort_key <= COALESCE(pub.max_published + 1, 1)
                AND gap.end_sort_key >= q.chapter_sort_key - 1
            )
            OR (
              q.chapter_sort_key <= 1.5
              AND pub.max_published IS NULL
              AND NOT EXISTS (
                SELECT 1
                FROM importer_chapter_mappings predecessor_mapping
                WHERE predecessor_mapping.work_id = (q.payload->>'workId')::uuid
                  AND predecessor_mapping.chapter_sort_key < q.chapter_sort_key
                  AND predecessor_mapping.is_gap = false
                  AND predecessor_mapping.status NOT IN ('STAGED', 'WAITING_FOR_GAP')
              )
            )
          )`;

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
    const res = await pool.query(`
      SELECT q.id, q.source, q.chapter_sort_key, pub.max_published, q.payload->>'workId' as work_id
      FROM importer_queue q
      LEFT JOIN LATERAL (
        SELECT MAX(c.number) AS max_published
        FROM chapters c
        WHERE c.work_id = (q.payload->>'workId')::uuid
          AND c.published_at IS NOT NULL
      ) pub ON TRUE
      WHERE q.status = 'QUEUED' 
        AND q.source IN ('nebulosascan', 'apenasumafa', 'montetai')
        ${CANONICAL_FRONTIER_CLAIM_FILTER}
      LIMIT 10
    `);
    console.log(JSON.stringify(res.rows, null, 2));
  } catch (err) {
    console.error(err.message);
  }
  await pool.end();
}
run();
