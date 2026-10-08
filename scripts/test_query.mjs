import { config } from 'dotenv';
config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });
import { Client } from 'pg';
import fs from 'fs';
const db = new Client({
  host: process.env.YUGABYTE_HOST, port: parseInt(process.env.YUGABYTE_PORT),
  user: process.env.YUGABYTE_USER, password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { ca: fs.readFileSync(process.env.YUGABYTE_SSL_CERT).toString(), rejectUnauthorized: false }
});
await db.connect();

const CANONICAL_PUBLISHED_CLAIM_FILTER = `
          AND NOT EXISTS (
            SELECT 1
            FROM chapters canonical
            WHERE canonical.work_id = (q.payload->>'workId')::uuid
              AND canonical.published_at IS NOT NULL
              AND (
                canonical.number = NULLIF(q.payload->>'chapterNumber', '')::numeric
                OR canonical.number = q.chapter_sort_key
              )
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
                FROM importer_queue predecessor
                WHERE predecessor.task_type = 'IMPORT_CHAPTER'
                  AND predecessor.payload->>'workId' = q.payload->>'workId'
                  AND predecessor.chapter_sort_key < q.chapter_sort_key
                  AND predecessor.status IN ('QUEUED', 'RETRY', 'IMPORTING')
              )
            )
          )
`;

const SOURCE_EXECUTION_ELIGIBILITY_SQL = `(
  (
    s.status = 'ACTIVE'
    AND (
      s.blocked_reason IS NULL
      OR s.blocked_details->>'probe_success' = 'true'
      OR (s.cooldown_until IS NOT NULL AND s.cooldown_until <= NOW())
    )
  )
  OR (s.status = 'COOLDOWN' AND s.cooldown_until <= NOW())
  OR (s.status = 'RATE_LIMITED' AND s.cooldown_until <= NOW())
)`;

const query = `
      WITH to_lock AS (
        SELECT q.id
        FROM importer_queue q
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
          AND s.enabled = true
          AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          ${CANONICAL_PUBLISHED_CLAIM_FILTER}
          ${CANONICAL_FRONTIER_CLAIM_FILTER}
          AND q.priority >= 100
          AND (q.payload->>'workId') = '00acfed7-ad54-4e2d-8a81-6b1ed0a5c39b'
        ORDER BY q.chapter_sort_key ASC NULLS LAST
        LIMIT 1
      )
      SELECT * FROM to_lock;
`;
const res = await db.query(query);
console.log(res.rows);
process.exit(0);
