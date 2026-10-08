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

  console.log("Testing CTE...");
  const start = Date.now();
  const res = await pool.query(`
    WITH candidate_jobs AS (
      SELECT 
        q.id,
        q.chapter_sort_key,
        (q.payload->>'workId') as work_id_text,
        (q.payload->>'workId')::uuid as work_id_uuid,
        COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key) as chapter_number
      FROM importer_queue q
      LEFT JOIN importer_sources s ON q.source = s.id
      WHERE q.status IN ('QUEUED', 'RETRY')
        AND q.task_type = 'IMPORT_CHAPTER'
        AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
        AND q.priority >= 50
        AND s.enabled = true
        AND (
          (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
          OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
        )
    ),
    active_predecessors AS (
      SELECT 
        (q.payload->>'workId') as work_id_text,
        q.chapter_sort_key,
        COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key) as chapter_number
      FROM importer_queue q
      WHERE q.status IN ('QUEUED', 'RETRY', 'IMPORTING')
        AND q.task_type = 'IMPORT_CHAPTER'
    )
    SELECT COUNT(*) as eligible_cnt
    FROM candidate_jobs c
    WHERE NOT EXISTS (
      SELECT 1 FROM chapters canonical_chapter
      WHERE canonical_chapter.work_id = c.work_id_uuid
        AND canonical_chapter.published_at IS NOT NULL
        AND canonical_chapter.number = c.chapter_number
    ) AND NOT EXISTS (
      SELECT 1 FROM active_predecessors p
      WHERE p.work_id_text = c.work_id_text
        AND p.chapter_sort_key < c.chapter_sort_key
        AND NOT EXISTS (
          SELECT 1 FROM chapters predecessor_canonical
          WHERE predecessor_canonical.work_id = c.work_id_uuid
            AND predecessor_canonical.published_at IS NOT NULL
            AND predecessor_canonical.number = p.chapter_number
        )
    );
  `);
  console.log("Result:", res.rows[0]);
  console.log("Time:", Date.now() - start, "ms");
  await pool.end();
}
run().catch(console.error);
