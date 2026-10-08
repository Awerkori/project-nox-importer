import fs from 'fs';

const path = 'src/core/scheduler/admission-controller.ts';
let code = fs.readFileSync(path, 'utf8');

const target = `         source_window AS MATERIALIZED (
           -- Keep the hot path bounded by source and use the existing
           -- (source,status,task_type,created_at) access path.  Ordering by
           -- priority/chapter here looked semantically attractive but forced
           -- YSQL to sort a full per-source backlog before applying LIMIT;
           -- with a large source that held a bounded pool client for the
           -- statement timeout and starved claims.  The later bounded
           -- frontier/contiguity pass still decides canonical executability.
           SELECT q.*
           FROM eligible_sources s
           CROSS JOIN LATERAL (
             SELECT q.*
             FROM importer_queue q
             WHERE q.source = s.id
               AND q.task_type = 'IMPORT_CHAPTER'
               AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())\${includePaused ? " OR q.status = 'PAUSED_BY_STAFF'" : ''})
               AND q.attempts < COALESCE(q.max_attempts, 7)
               AND q.priority >= 75 AND q.priority < 100
               AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
               AND NOT ((q.payload->>'workId') = ANY($1::text[]))
               AND q.payload->>'workId' IS NOT NULL
               AND NOT ((q.payload->>'workId') = ANY($7::text[]))
             ORDER BY q.priority DESC, q.chapter_sort_key ASC
             LIMIT $5
           ) q`;

const replacement = `         source_window AS MATERIALIZED (
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
           LIMIT $5 * 10`;

if (!code.includes(target)) {
  console.log("Target not found!");
  process.exit(1);
}

fs.writeFileSync(path, code.replace(target, replacement));
console.log("Patched successfully!");
