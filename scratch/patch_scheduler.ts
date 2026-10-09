import fs from 'fs';
const file = 'src/core/scheduler/work-affinity-scheduler.ts';
let content = fs.readFileSync(file, 'utf8');

// Revert PR 355 change in executeClaimCatalogQuery
content = content.replace(
`        CROSS JOIN LATERAL (
          SELECT q.id
          FROM importer_queue q
          JOIN works w ON w.id = (q.payload->>'workId')::uuid
          WHERE q.source = s.id
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
            AND q.task_type = 'IMPORT_CHAPTER'
            AND q.attempts < COALESCE(q.max_attempts, 7)
            AND w.published = true
          -- Uses idx_importer_queue_fetch; canonical selection remains below.
          ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
          LIMIT $6
        ) candidate`,
`        CROSS JOIN LATERAL (
          SELECT q.id
          FROM importer_queue q
          WHERE q.source = s.id
            AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
            AND q.task_type = 'IMPORT_CHAPTER'
            AND q.attempts < COALESCE(q.max_attempts, 7)
          -- Uses idx_importer_queue_fetch; canonical selection remains below.
          ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
          LIMIT $6
        ) candidate`
);

// We must also add w.published = true back to the to_lock WHERE clause if it's missing!
// Let's check where to put it. We can just replace AND s.enabled = true with AND s.enabled = true AND w.published = true
content = content.replace(
`          AND s.enabled = true
          AND \${SOURCE_EXECUTION_ELIGIBILITY_SQL}`,
`          AND s.enabled = true
          AND w.published = true
          AND \${SOURCE_EXECUTION_ELIGIBILITY_SQL}`
);

fs.writeFileSync(file, content);
