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
    const SOURCE_EXECUTION_ELIGIBILITY_SQL = `
      (
        (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
        OR (s.status IN ('COOLDOWN','PROBING','DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
      )
    `;

    const q = `
         EXPLAIN ANALYZE WITH eligible_sources AS MATERIALIZED (
           SELECT s.*
           FROM importer_sources s
           WHERE s.enabled = true
             AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
             AND s.id = ANY($3::text[])
         ),
         source_window AS MATERIALIZED (
           SELECT q.*
           FROM eligible_sources s
           CROSS JOIN LATERAL (
             SELECT q.*
             FROM importer_queue q
             WHERE q.source = s.id
               AND q.task_type = 'IMPORT_CHAPTER'
               AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()) OR q.status = 'PAUSED_BY_STAFF')
               AND q.attempts < COALESCE(q.max_attempts, 7)
               AND q.priority >= 75 AND q.priority < 100
               AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
               AND NOT ((q.payload->>'workId') = ANY($1::text[]))
               AND q.payload->>'workId' IS NOT NULL
               AND NOT ((q.payload->>'workId') = ANY($4::text[]))
             ORDER BY q.created_at ASC
             LIMIT 1000
           ) q
         ),
         candidate_works AS MATERIALIZED (
           SELECT 
             q.payload->>'workId' AS work_id,
             MIN(q.chapter_sort_key) as min_chapter_sort_key,
             MIN(q.next_run_at) as min_next_run_at,
             (array_agg(q.id ORDER BY q.chapter_sort_key ASC))[1] as id
           FROM source_window q
           JOIN works w ON w.id = (q.payload->>'workId')::uuid
           WHERE w.published IS TRUE
           GROUP BY q.payload->>'workId'
           ORDER BY MIN(q.next_run_at) ASC, MIN(q.chapter_sort_key) ASC NULLS LAST
           LIMIT 100
         )
         SELECT q.payload->>'workId' as "workId", q.source, q.chapter_sort_key, q.priority
         FROM candidate_works cw
         JOIN importer_queue q 
           ON (q.payload->>'workId') = cw.work_id 
           AND q.chapter_sort_key = cw.min_chapter_sort_key
           AND q.id = cw.id
         JOIN works w ON w.id = cw.work_id::uuid
         CROSS JOIN LATERAL (
           SELECT MAX(c.number) AS max_published
           FROM chapters c
           WHERE c.work_id = cw.work_id::uuid
             AND c.published_at IS NOT NULL
         ) pub
         WHERE q.task_type = 'IMPORT_CHAPTER'
           AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()) OR q.status = 'PAUSED_BY_STAFF')
           AND w.published IS TRUE
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
                 WHERE predecessor_canonical.work_id = cw.work_id::uuid
                   AND predecessor_canonical.published_at IS NOT NULL
                   AND (
                     predecessor_canonical.number = NULLIF(predecessor.payload->>'chapterNumber', '')::numeric
                     OR predecessor_canonical.number = predecessor.chapter_sort_key
                   )
               )
           )
           AND (
             (pub.max_published IS NOT NULL AND q.chapter_sort_key <= pub.max_published + 1.5)
             OR (
               pub.max_published IS NOT NULL
               AND EXISTS (
                 SELECT 1
                 FROM importer_confirmed_gaps gap
                 WHERE gap.work_id = cw.work_id::uuid
                   AND gap.start_sort_key <= pub.max_published + 1
                   AND gap.end_sort_key >= q.chapter_sort_key - 1
               )
             )
             OR (
               pub.max_published IS NULL
               AND q.chapter_sort_key <= 1.5
               AND NOT EXISTS (
                 SELECT 1
                 FROM importer_chapter_mappings predecessor_mapping
                 WHERE predecessor_mapping.work_id = cw.work_id::uuid
                   AND predecessor_mapping.chapter_sort_key < q.chapter_sort_key
                   AND predecessor_mapping.is_gap = false
                   AND predecessor_mapping.status NOT IN ('STAGED', 'WAITING_FOR_GAP')
               )
             )
           )
           AND NOT EXISTS (
             SELECT 1
             FROM chapters canonical_chapter
             WHERE canonical_chapter.work_id = cw.work_id::uuid
               AND canonical_chapter.published_at IS NOT NULL
               AND (
                 canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
               )
           )
           AND NOT EXISTS (
             SELECT 1
             FROM importer_chapter_mappings staged_frontier
             WHERE staged_frontier.work_id = cw.work_id::uuid
               AND staged_frontier.chapter_sort_key = q.chapter_sort_key
               AND staged_frontier.status IN ('STAGED', 'WAITING_FOR_GAP')
           )
         LIMIT $2;
    `;
    const res = await pool.query(q, [
      [], // $1 activeIds
      1, // $2 backfillSlotsAvailable
      ['taimumangas'], // $3 p1SourceWindow
      [] // $4 deadWorks
    ]);
    console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  } finally {
    await pool.end();
  }
}
main();
