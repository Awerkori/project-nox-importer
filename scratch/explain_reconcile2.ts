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
               s.blocked_reason AS source_blocked_reason, s.blocked_details AS source_blocked_details
        FROM unnest($1::text[], $2::text[]) AS w(work_id, source)
        CROSS JOIN LATERAL (
          SELECT COUNT(*) FILTER (WHERE q.status='QUEUED' AND q.attempts < COALESCE(q.max_attempts,7)) AS queued_cnt,
            COUNT(*) FILTER (WHERE q.status='IMPORTING') AS importing_cnt,
            COUNT(*) FILTER (WHERE q.status='PAUSED_BY_STAFF') AS paused_cnt,
            COUNT(*) FILTER (WHERE q.status='RETRY' AND q.attempts < COALESCE(q.max_attempts,7)) AS retry_cnt,
            MIN(q.chapter_sort_key) FILTER (WHERE q.status='QUEUED' AND q.attempts < COALESCE(q.max_attempts,7)) AS min_queued,
            MIN(q.chapter_sort_key) FILTER (WHERE q.status IN ('QUEUED','RETRY','PAUSED_BY_STAFF') AND q.attempts < COALESCE(q.max_attempts,7)) AS min_sort_key,
            0 AS claimable_cnt
          FROM importer_queue q
          LEFT JOIN importer_sources s ON s.id = q.source
          WHERE q.task_type='IMPORT_CHAPTER' AND q.payload->>'workId'=w.work_id
        ) q
        CROSS JOIN LATERAL (
          SELECT COUNT(*) AS pub_cnt, COALESCE(MAX(number),-1) AS max_pub
          FROM chapters WHERE work_id = w.work_id::uuid AND published_at IS NOT NULL
        ) p
        CROSS JOIN LATERAL (
          SELECT COUNT(*) FILTER (WHERE status='STAGED') AS staged_cnt,
                 COUNT(*) FILTER (WHERE status='WAITING_FOR_GAP') AS waiting_gap_cnt
          FROM importer_chapter_mappings
          WHERE work_id = w.work_id::uuid
        ) m
        LEFT JOIN importer_sources s ON s.id = w.source;
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
