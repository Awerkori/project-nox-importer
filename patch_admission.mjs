import fs from 'fs';

const path = 'src/core/scheduler/admission-controller.ts';
let code = fs.readFileSync(path, 'utf8');

const target = `        ), source_window AS MATERIALIZED (
          SELECT q.*
          FROM eligible_sources s
          CROSS JOIN LATERAL (
            SELECT q.*
            FROM importer_queue q
            WHERE q.source = s.id
              AND q.task_type = 'IMPORT_CHAPTER'
              AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())\${includePaused ? " OR q.status = 'PAUSED_BY_STAFF'" : ''})
              AND q.attempts < COALESCE(q.max_attempts,7)
              AND q.priority >= \${isP1 ? 75 : 50} AND q.priority < \${maxPriority}
              AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
              AND NOT ((q.payload->>'workId') = ANY($2::text[]))
              AND q.payload->>'workId' IS NOT NULL
              AND NOT ((q.payload->>'workId') = ANY($7::text[]))
            ORDER BY q.priority DESC, q.chapter_sort_key ASC
            LIMIT $5
          ) q`;

const replacement = `        ), source_window AS MATERIALIZED (
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
            AND NOT ((q.payload->>'workId') = ANY($7::text[]))
          ORDER BY q.priority DESC, q.chapter_sort_key ASC
          LIMIT 160`;

if (!code.includes(target)) {
  console.log("Target not found!");
  process.exit(1);
}

fs.writeFileSync(path, code.replace(target, replacement));
console.log("Patched successfully!");
