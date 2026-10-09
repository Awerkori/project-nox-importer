import fs from 'fs';
const file = 'src/core/scheduler/admission-controller.ts';
let content = fs.readFileSync(file, 'utf8');

content = content.replace(
`         queued_retry AS (
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
         )`,
`         queued_retry AS (
           SELECT candidate.*
           FROM eligible_sources s
           CROSS JOIN LATERAL (
             SELECT q.*
             FROM importer_queue q
             WHERE q.source = s.id
               AND q.task_type = 'IMPORT_CHAPTER'
               AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
               AND q.attempts < COALESCE(q.max_attempts, 7)
               AND q.priority >= 75 AND q.priority < 100
               AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
               AND NOT ((q.payload->>'workId') = ANY($1::text[]))
               AND q.payload->>'workId' IS NOT NULL
               AND NOT ((q.payload->>'workId') = ANY($7::text[]))
             ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
             LIMIT $5
           ) candidate
         )`
);

content = content.replace(
`         \${includePaused ? \`, paused AS (
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
         )\` : ''}`,
`         \${includePaused ? \`, paused AS (
           SELECT candidate.*
           FROM eligible_sources s
           CROSS JOIN LATERAL (
             SELECT q.*
             FROM importer_queue q
             WHERE q.source = s.id
               AND q.task_type = 'IMPORT_CHAPTER'
               AND q.status = 'PAUSED_BY_STAFF'
               AND q.attempts < COALESCE(q.max_attempts, 7)
               AND q.priority >= 75 AND q.priority < 100
               AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
               AND NOT ((q.payload->>'workId') = ANY($1::text[]))
               AND q.payload->>'workId' IS NOT NULL
               AND NOT ((q.payload->>'workId') = ANY($7::text[]))
             ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
             LIMIT $5
           ) candidate
         )\` : ''}`
);

fs.writeFileSync(file, content);
