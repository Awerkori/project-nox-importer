import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

async function main() {
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    max: 1,
    ssl: { rejectUnauthorized: false }
  });
  try {
    const q = `
        EXPLAIN ANALYZE SELECT w.work_id, q.*, p.*, m.*, s.status AS source_status, s.cooldown_until,
               s.blocked_reason AS source_blocked_reason, s.blocked_details AS source_blocked_details,
               c.claimable_cnt
        FROM unnest($1::text[], $2::text[]) AS w(work_id, source)
        CROSS JOIN LATERAL (
          SELECT COUNT(*) AS pub_cnt, COALESCE(MAX(number),-1) AS max_pub
          FROM chapters WHERE work_id = w.work_id::uuid AND published_at IS NOT NULL
        ) p
        CROSS JOIN LATERAL (
          SELECT COUNT(*) FILTER (WHERE q.status='QUEUED' AND q.attempts < COALESCE(q.max_attempts,7)) AS queued_cnt,
            COUNT(*) FILTER (WHERE q.status='IMPORTING') AS importing_cnt,
            COUNT(*) FILTER (WHERE q.status='PAUSED_BY_STAFF') AS paused_cnt,
            COUNT(*) FILTER (WHERE q.status='RETRY' AND q.attempts < COALESCE(q.max_attempts,7)) AS retry_cnt,
            MIN(q.chapter_sort_key) FILTER (WHERE q.status='QUEUED' AND q.attempts < COALESCE(q.max_attempts,7)) AS min_queued,
            MIN(q.chapter_sort_key) FILTER (WHERE q.status IN ('QUEUED','RETRY','PAUSED_BY_STAFF') AND q.attempts < COALESCE(q.max_attempts,7)) AS min_sort_key
          FROM importer_queue q
          WHERE q.task_type='IMPORT_CHAPTER' AND q.payload->>'workId'=w.work_id
        ) q
        CROSS JOIN LATERAL (
          SELECT COUNT(*) FILTER (WHERE status='STAGED') AS staged_cnt,
                 COUNT(*) FILTER (WHERE status='WAITING_FOR_GAP') AS waiting_gap_cnt
          FROM importer_chapter_mappings
          WHERE work_id = w.work_id::uuid
        ) m
        LEFT JOIN importer_sources s ON s.id = w.source
        LEFT JOIN LATERAL (
          SELECT CASE WHEN EXISTS (
            SELECT 1
            FROM importer_queue candidate
            JOIN importer_sources s_candidate ON s_candidate.id = candidate.source
            WHERE candidate.task_type = 'IMPORT_CHAPTER'
              AND candidate.payload->>'workId' = w.work_id
              AND (candidate.status='QUEUED' OR (candidate.status='RETRY' AND candidate.next_run_at <= NOW()))
              AND candidate.attempts < COALESCE(candidate.max_attempts,7)
              AND s_candidate.enabled = true
              AND (
                (s_candidate.status = 'ACTIVE' AND (s_candidate.blocked_reason IS NULL OR s_candidate.blocked_details->>'probe_success' = 'true' OR s_candidate.blocked_details->>'recovered_at' IS NOT NULL))
                OR (s_candidate.status IN ('COOLDOWN','PROBING','DEGRADED') AND (s_candidate.blocked_reason IS NULL OR s_candidate.blocked_details->>'probe_success' = 'true' OR s_candidate.blocked_details->>'recovered_at' IS NOT NULL) AND (s_candidate.cooldown_until IS NULL OR s_candidate.cooldown_until <= NOW()))
              )
              AND NOT EXISTS (
                SELECT 1 FROM chapters canonical_chapter
                WHERE canonical_chapter.work_id = w.work_id::uuid
                  AND canonical_chapter.published_at IS NOT NULL
                  AND (canonical_chapter.number = COALESCE(NULLIF(candidate.payload->>'chapterNumber', '')::numeric, candidate.chapter_sort_key))
              )
              AND NOT EXISTS (
                SELECT 1 FROM importer_queue predecessor
                WHERE predecessor.task_type = 'IMPORT_CHAPTER'
                  AND predecessor.payload->>'workId' = candidate.payload->>'workId'
                  AND predecessor.chapter_sort_key < candidate.chapter_sort_key
                  AND predecessor.status IN ('QUEUED', 'RETRY', 'IMPORTING')
                  AND NOT EXISTS (
                    SELECT 1 FROM chapters predecessor_canonical
                    WHERE predecessor_canonical.work_id = w.work_id::uuid
                      AND predecessor_canonical.published_at IS NOT NULL
                      AND (predecessor_canonical.number = NULLIF(predecessor.payload->>'chapterNumber', '')::numeric OR predecessor_canonical.number = predecessor.chapter_sort_key)
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM importer_chapter_mappings staged_frontier
                WHERE staged_frontier.work_id = w.work_id::uuid
                  AND staged_frontier.chapter_sort_key = candidate.chapter_sort_key
                  AND staged_frontier.status IN ('STAGED', 'WAITING_FOR_GAP')
              )
              AND (
                (p.max_pub >= 0 AND candidate.chapter_sort_key <= p.max_pub + 1.5)
                OR EXISTS (
                  SELECT 1 FROM importer_confirmed_gaps gap
                  WHERE gap.work_id = w.work_id::uuid
                    AND gap.start_sort_key <= COALESCE(NULLIF(p.max_pub, -1) + 1, 1)
                    AND gap.end_sort_key >= candidate.chapter_sort_key - 1
                )
                OR (
                  p.max_pub = -1
                  AND candidate.chapter_sort_key <= 1.5
                  AND NOT EXISTS (
                    SELECT 1 FROM importer_chapter_mappings predecessor_mapping
                    WHERE predecessor_mapping.work_id = w.work_id::uuid
                      AND predecessor_mapping.chapter_sort_key < candidate.chapter_sort_key
                      AND predecessor_mapping.is_gap = false
                      AND predecessor_mapping.status NOT IN ('STAGED', 'WAITING_FOR_GAP')
                  )
                )
              )
          ) THEN 1 ELSE 0 END AS claimable_cnt
        ) c ON TRUE;
    `;
    const res = await pool.query(q, [
      ['2455b9b4-9292-4d6b-b9c1-2307aadde6a7', 'f079e2eb-a5d6-43c9-a06f-81a95f0a0e8d'],
      ['taimumangas', 'taimumangas']
    ]);
    console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  } finally {
    await pool.end();
  }
}
main();
