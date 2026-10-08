import { Pool } from 'pg';
const pool = new Pool({ connectionString: process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/nox' });

async function run() {
  const res = await pool.query(`
    SELECT COUNT(*) as eligible_cnt
    FROM importer_queue q
    LEFT JOIN importer_sources s ON q.source = s.id
    WHERE q.status IN ('QUEUED', 'RETRY') 
      AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
      AND q.task_type = 'IMPORT_CHAPTER'
      AND s.enabled = true AND (
          (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
          OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
      )
      AND NOT EXISTS (
        SELECT 1
        FROM chapters canonical_chapter
        WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
          AND canonical_chapter.published_at IS NOT NULL
          AND (
            canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
          )
      )
  `);
  console.log('Filtered eligible_cnt:', res.rows[0].eligible_cnt);

  await pool.end();
}
run().catch(console.error);
