import * as fs from 'fs';

let content = fs.readFileSync('src/core/scheduler/admission-controller.ts', 'utf8');

const p1Before = `         source_window AS MATERIALIZED (
           SELECT q.*
           FROM importer_queue q
           JOIN eligible_sources s ON s.id = q.source
           WHERE q.task_type = 'IMPORT_CHAPTER'
             AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())\${includePaused ? " OR q.status = 'PAUSED_BY_STAFF'" : ''})
             AND q.attempts < COALESCE(q.max_attempts, 7)
             AND q.priority >= 75 AND q.priority < 100
             AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
             AND NOT ((q.payload->>'workId') = ANY($1::text[]))
             AND q.payload->>'workId' IS NOT NULL
             AND NOT ((q.payload->>'workId') = ANY($7::text[]))
           ORDER BY q.priority DESC, q.chapter_sort_key ASC
           LIMIT $5
         ),`;

const p1After = `         queued_retry AS (
           SELECT q.*
           FROM importer_queue q
           JOIN eligible_sources s ON s.id = q.source
           WHERE q.task_type = 'IMPORT_CHAPTER'
             AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
             AND q.attempts < COALESCE(q.max_attempts, 7)
             AND q.priority >= 75 AND q.priority < 100
             AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
             AND NOT ((q.payload->>'workId') = ANY($1::text[]))
             AND q.payload->>'workId' IS NOT NULL
             AND NOT ((q.payload->>'workId') = ANY($7::text[]))
           ORDER BY q.priority DESC, q.chapter_sort_key ASC
           LIMIT $5
         )
         \${includePaused ? \`, paused AS (
           SELECT q.*
           FROM importer_queue q
           JOIN eligible_sources s ON s.id = q.source
           WHERE q.task_type = 'IMPORT_CHAPTER'
             AND q.status = 'PAUSED_BY_STAFF'
             AND q.attempts < COALESCE(q.max_attempts, 7)
             AND q.priority >= 75 AND q.priority < 100
             AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
             AND NOT ((q.payload->>'workId') = ANY($1::text[]))
             AND q.payload->>'workId' IS NOT NULL
             AND NOT ((q.payload->>'workId') = ANY($7::text[]))
           ORDER BY q.priority DESC, q.chapter_sort_key ASC
           LIMIT $5
         )\` : ''}
         , source_window AS MATERIALIZED (
           SELECT * FROM queued_retry
           \${includePaused ? 'UNION ALL SELECT * FROM paused' : ''}
           ORDER BY priority DESC, chapter_sort_key ASC
           LIMIT $5
         ),`;

const p2Before = `        ), source_window AS MATERIALIZED (
          SELECT q.*
          FROM importer_queue q
          JOIN eligible_sources s ON s.id = q.source
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())\${includePaused ? " OR q.status = 'PAUSED_BY_STAFF'" : ''})
            AND q.attempts < COALESCE(q.max_attempts,7)
            AND q.priority >= \${isP1 ? 75 : 50} AND q.priority < \${maxPriority}
            AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
            AND NOT ((q.payload->>'workId') = ANY($2::text[]))
            AND q.payload->>'workId' IS NOT NULL
            AND NOT ((q.payload->>'workId') = ANY($6::text[]))
          ORDER BY q.priority DESC, q.chapter_sort_key ASC
          LIMIT 160
        ), queue_candidates AS MATERIALIZED (`;

const p2After = `        ), queued_retry AS (
          SELECT q.*
          FROM importer_queue q
          JOIN eligible_sources s ON s.id = q.source
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
            AND q.attempts < COALESCE(q.max_attempts,7)
            AND q.priority >= \${isP1 ? 75 : 50} AND q.priority < \${maxPriority}
            AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
            AND NOT ((q.payload->>'workId') = ANY($2::text[]))
            AND q.payload->>'workId' IS NOT NULL
            AND NOT ((q.payload->>'workId') = ANY($6::text[]))
          ORDER BY q.priority DESC, q.chapter_sort_key ASC
          LIMIT 160
        )
        \${includePaused ? \`, paused AS (
          SELECT q.*
          FROM importer_queue q
          JOIN eligible_sources s ON s.id = q.source
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND q.status = 'PAUSED_BY_STAFF'
            AND q.attempts < COALESCE(q.max_attempts,7)
            AND q.priority >= \${isP1 ? 75 : 50} AND q.priority < \${maxPriority}
            AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
            AND NOT ((q.payload->>'workId') = ANY($2::text[]))
            AND q.payload->>'workId' IS NOT NULL
            AND NOT ((q.payload->>'workId') = ANY($6::text[]))
          ORDER BY q.priority DESC, q.chapter_sort_key ASC
          LIMIT 160
        )\` : ''}
        , source_window AS MATERIALIZED (
          SELECT * FROM queued_retry
          \${includePaused ? 'UNION ALL SELECT * FROM paused' : ''}
          ORDER BY priority DESC, chapter_sort_key ASC
          LIMIT 160
        ), queue_candidates AS MATERIALIZED (`;

if (content.includes(p1Before) && content.includes(p2Before)) {
  content = content.replace(p1Before, p1After);
  content = content.replace(p2Before, p2After);
  fs.writeFileSync('src/core/scheduler/admission-controller.ts', content);
  console.log("Patched successfully!");
} else {
  console.log("Could not find matching strings to replace!");
  if (!content.includes(p1Before)) console.log("p1Before not found");
  if (!content.includes(p2Before)) console.log("p2Before not found");
}
