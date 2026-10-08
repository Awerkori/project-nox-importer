const fs = require('fs');

let code = fs.readFileSync('src/core/scheduler/work-affinity-scheduler.ts', 'utf8');

const catalogReplacement = `
    const allowedSourcesFilter = opts.allowedSources && opts.allowedSources.length > 0 ? \`AND q.source = ANY($1::text[])\` : \`\`;
    const disallowedWorkIdsFilter = opts.disallowedWorkIds && opts.disallowedWorkIds.length > 0 ? \`AND NOT ((q.payload->>'workId') = ANY($2::text[]))\` : \`\`;
    const disallowedChapterKeysFilter = opts.disallowedChapterKeys && opts.disallowedChapterKeys.length > 0 ? \`AND NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($3::text[]))\` : \`\`;

    const query = \`
      WITH q_candidates AS (
        SELECT id, payload, source, chapter_sort_key, next_run_at, priority
        FROM importer_queue
        WHERE (status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW()))
          AND task_type = 'IMPORT_CHAPTER'
          AND attempts < COALESCE(max_attempts, 7)
      ),
      to_lock AS (
        SELECT q_base.id
        FROM importer_queue q_base
        JOIN q_candidates q ON q_base.id = q.id
        JOIN works w ON w.id = (q.payload->>'workId')::uuid
        JOIN importer_sources s ON s.id = q.source
        LEFT JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = (q.payload->>'workId')::uuid
            AND c.published_at IS NOT NULL
        ) pub ON TRUE
        WHERE s.enabled = true
          AND \${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          AND w.published = true
          \${CANONICAL_PUBLISHED_CLAIM_FILTER}
          \${CANONICAL_FRONTIER_CLAIM_FILTER}
          \${allowedSourcesFilter}
          \${disallowedWorkIdsFilter}
          \${disallowedChapterKeysFilter}
          \${CANONICAL_ACTIVE_CLAIM_FILTER}
        ORDER BY
          q.priority DESC, 
          q.chapter_sort_key ASC NULLS LAST, 
          q.next_run_at ASC
        FOR UPDATE OF q_base SKIP LOCKED
        LIMIT 1
      )
      UPDATE importer_queue q
      SET status = 'IMPORTING',
          locked_by = $4,
          locked_at = NOW(),
          lease_expires_at = NOW() + ($5::text || ' minutes')::interval,
          attempts = q.attempts + 1,
          updated_at = NOW()
      FROM to_lock
      WHERE q.id = to_lock.id
      RETURNING q.id, q.task_type, q.source, q.priority, q.payload, q.dedupe_key,
                q.status, q.attempts, q.max_attempts, q.locked_by, q.locked_at,
                q.lease_expires_at, q.next_run_at, q.last_error, q.chapter_sort_key;
    \`;
`;

const searchPoint = code.indexOf("private async executeClaimCatalogQuery");
const qStart1 = "    const query = `\n      WITH q_candidates AS (";
const startIndex = code.indexOf(qStart1, searchPoint);
const endIndexStr = "q.lease_expires_at, q.next_run_at, q.last_error, q.chapter_sort_key;\n    `;";
const endIndex = code.indexOf(endIndexStr, startIndex);

if (startIndex > -1 && endIndex > -1) {
  code = code.slice(0, startIndex) + catalogReplacement.trim() + code.slice(endIndex + endIndexStr.length);
  console.log("Patched executeClaimCatalogQuery");
  fs.writeFileSync('src/core/scheduler/work-affinity-scheduler.ts', code);
} else {
  console.log("Not found", startIndex, endIndex);
}
