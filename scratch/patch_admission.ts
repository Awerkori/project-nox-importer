import fs from 'fs';
const file = 'src/core/scheduler/admission-controller.ts';
let content = fs.readFileSync(file, 'utf8');

// Replace queued_retry
content = content.replace(
`        ), queued_retry AS (
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
        )`,
`        ), queued_retry AS (
          SELECT candidate.*
          FROM eligible_sources s
          CROSS JOIN LATERAL (
            SELECT q.*
            FROM importer_queue q
            WHERE q.source = s.id
              AND q.task_type = 'IMPORT_CHAPTER'
              AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
              AND q.attempts < COALESCE(q.max_attempts,7)
              AND q.priority >= \${isP1 ? 75 : 50} AND q.priority < \${maxPriority}
              AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
              AND NOT ((q.payload->>'workId') = ANY($2::text[]))
              AND q.payload->>'workId' IS NOT NULL
              AND NOT ((q.payload->>'workId') = ANY($6::text[]))
            ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
            LIMIT 160
          ) candidate
        )`
);

// Replace paused
content = content.replace(
`        \${includePaused ? \`, paused AS (
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
        )\` : ''}`,
`        \${includePaused ? \`, paused AS (
          SELECT candidate.*
          FROM eligible_sources s
          CROSS JOIN LATERAL (
            SELECT q.*
            FROM importer_queue q
            WHERE q.source = s.id
              AND q.task_type = 'IMPORT_CHAPTER'
              AND q.status = 'PAUSED_BY_STAFF'
              AND q.attempts < COALESCE(q.max_attempts,7)
              AND q.priority >= \${isP1 ? 75 : 50} AND q.priority < \${maxPriority}
              AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
              AND NOT ((q.payload->>'workId') = ANY($2::text[]))
              AND q.payload->>'workId' IS NOT NULL
              AND NOT ((q.payload->>'workId') = ANY($6::text[]))
            ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
            LIMIT 160
          ) candidate
        )\` : ''}`
);

fs.writeFileSync(file, content);
