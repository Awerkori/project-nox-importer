import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: {
    rejectUnauthorized: false
  }
});

async function run() {
  const query = `
      SELECT 
        COUNT(CASE WHEN q.status IN ('QUEUED', 'RETRY') AND (q.next_run_at IS NULL OR q.next_run_at <= NOW()) AND q.task_type = 'IMPORT_CHAPTER' AND q.priority >= 50 AND s.enabled = true AND (
          (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
          OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
        ) AND NOT EXISTS (
          SELECT 1 FROM chapters canonical_chapter
          WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
            AND canonical_chapter.published_at IS NOT NULL
            AND canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
        ) AND NOT EXISTS (
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
                  AND (
                    predecessor_canonical.number = COALESCE(NULLIF(predecessor.payload->>'chapterNumber', '')::numeric, predecessor.chapter_sort_key)
                  )
              )
        ) THEN 1 END) as eligible_cnt
      FROM importer_queue q
      LEFT JOIN importer_sources s ON q.source = s.id
      WHERE q.status IN ('QUEUED', 'RETRY', 'IMPORTING')
  `;
  const res = await pool.query(query);
  console.log("Eligible count with ALL filters:", res.rows[0].eligible_cnt);
  await pool.end();
}
run().catch(console.error);
