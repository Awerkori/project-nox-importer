import fs from 'fs';
const file = 'src/core/scheduler/admission-controller.ts';
let code = fs.readFileSync(file, 'utf8');

const regex = /const loadP2Candidates = \(includePaused: boolean\) => this\.runQuery\(\s*`WITH queue_candidates AS MATERIALIZED \([\s\S]*?LIMIT \$2`,\s*\[[\s\S]*?\]\s*\);/;

const replacement = \`const loadP2Candidates = (includePaused: boolean) => this.runQuery(
        \\\`WITH eligible_sources AS MATERIALIZED (
          SELECT s.id
          FROM importer_sources s
          WHERE s.enabled = true
            AND \${SOURCE_EXECUTION_ELIGIBILITY_SQL}
        ), source_window AS MATERIALIZED (
          SELECT q.*
          FROM eligible_sources s
          CROSS JOIN LATERAL (
            SELECT q.*
            FROM importer_queue q
            WHERE q.source = s.id
              AND q.task_type = 'IMPORT_CHAPTER'
              AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())\${includePaused ? " OR q.status = 'PAUSED_BY_STAFF'" : ''})
              AND q.attempts < COALESCE(q.max_attempts,7)
              AND q.priority < 75
              AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
              AND q.payload->>'workId' IS NOT NULL
              AND NOT ((q.payload->>'workId') = ANY(\$1::text[]))
            ORDER BY q.created_at ASC
            LIMIT 32
          ) q
        ), queue_candidates AS MATERIALIZED (
          SELECT (q.payload->>'workId') as work_id, q.source, COUNT(*) as pending_jobs,
                 COUNT(CASE WHEN q.status = 'QUEUED' THEN 1 END) as queued_count,
                 COUNT(CASE WHEN q.status = 'PAUSED_BY_STAFF' THEN 1 END) as paused_count,
                 MIN(q.chapter_sort_key) as min_sort_key
          FROM source_window q
          WHERE NOT EXISTS (
             SELECT 1
             FROM chapters canonical_chapter
             WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
               AND canonical_chapter.published_at IS NOT NULL
               AND (
                 canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
               )
           )
          GROUP BY (q.payload->>'workId'), q.source
        )
        SELECT work_id, title, source, pending_jobs, queued_count, paused_count, min_sort_key, created_at
        FROM (
          SELECT q.*, w.title, w.created_at,
            ROW_NUMBER() OVER (
              PARTITION BY q.source
              ORDER BY w.created_at DESC
            ) AS source_rank
          FROM queue_candidates q
          JOIN works w ON w.id = q.work_id::uuid
          WHERE w.published IS FALSE
        ) ranked
        WHERE source_rank <= \$3
        ORDER BY created_at DESC
        LIMIT \$2\\\`,
        [
          activeIds.length > 0 ? activeIds : ['00000000-0000-0000-0000-000000000000'],
          newWorkSlotsAvailable * 3,
          4,
        ]
      );
\`.trim();

const newCode = code.replace(regex, replacement);
if (newCode === code) {
  console.log('No match found!');
} else {
  fs.writeFileSync(file, newCode);
  console.log('Replaced loadP2Candidates');
}
