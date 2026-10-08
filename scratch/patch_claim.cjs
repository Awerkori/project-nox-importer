const fs = require('fs');

let code = fs.readFileSync('src/core/scheduler/work-affinity-scheduler.ts', 'utf8');

// Replace claimSingleJob query
const claimSingleJobReplacement = `
      const disallowedChapterKeys = Array.from(this.inFlightChapterKeys);
      const isSingleWork = Boolean(opts.workId);
      const orderClause = isSingleWork
        ? \`ORDER BY q.chapter_sort_key ASC NULLS LAST\`
        : \`ORDER BY
          q.priority DESC, 
          q.chapter_sort_key ASC NULLS LAST, 
          q.next_run_at ASC\`;

      const minPriorityFilter = opts.minPriority != null ? \`AND priority >= $2::int\` : \`\`;
      const maxPriorityFilter = opts.maxPriority != null ? \`AND priority <= $10::int\` : \`\`;
      const workIdFilter = opts.workId ? \`AND (payload->>'workId') = $3::text\` : \`\`;
      const sortKeyFilter = opts.sortKey != null ? \`AND chapter_sort_key = $4::numeric\` : \`\`;
      const allowedWorkIdsFilter = opts.allowedWorkIds && opts.allowedWorkIds.length > 0 ? \`AND (payload->>'workId') = ANY($7::text[])\` : \`\`;
      
      const allowedSourcesFilter = opts.allowedSources && opts.allowedSources.length > 0 ? \`AND q.source = ANY($1::text[])\` : \`\`;
      const disallowedWorkIdsFilter = opts.disallowedWorkIds && opts.disallowedWorkIds.length > 0 ? \`AND NOT ((q.payload->>'workId') = ANY($8::text[]))\` : \`\`;
      const disallowedChapterKeysFilter = disallowedChapterKeys.length > 0 ? \`AND NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($9::text[]))\` : \`\`;

      const query = \`
      WITH q_candidates AS (
        SELECT id, payload, source, chapter_sort_key, next_run_at, priority
        FROM importer_queue
        WHERE (status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW()))
          AND task_type = 'IMPORT_CHAPTER'
          AND attempts < COALESCE(max_attempts, 7)
          \${minPriorityFilter}
          \${maxPriorityFilter}
          \${workIdFilter}
          \${sortKeyFilter}
          \${allowedWorkIdsFilter}
      ),
      to_lock AS (
        SELECT q_base.id
        FROM importer_queue q_base
        JOIN q_candidates q ON q_base.id = q.id
        JOIN importer_sources s ON s.id = q.source
        LEFT JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = (q.payload->>'workId')::uuid
            AND c.published_at IS NOT NULL
        ) pub ON TRUE
        WHERE s.enabled = true
          AND \${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          \${CANONICAL_PUBLISHED_CLAIM_FILTER}
          \${CANONICAL_FRONTIER_CLAIM_FILTER}
          \${allowedSourcesFilter}
          \${disallowedWorkIdsFilter}
          \${disallowedChapterKeysFilter}
          \${CANONICAL_ACTIVE_CLAIM_FILTER}
        \${orderClause}
        FOR UPDATE OF q_base SKIP LOCKED
        LIMIT 1
      )
      UPDATE importer_queue q_base
      SET status = 'IMPORTING',
          locked_by = $5,
          locked_at = NOW(),
          lease_expires_at = NOW() + ($6::text || ' minutes')::interval,
          attempts = q_base.attempts + 1,
          updated_at = NOW()
      FROM to_lock
      WHERE q_base.id = to_lock.id
      RETURNING q_base.id, q_base.task_type, q_base.source, q_base.priority, q_base.payload, q_base.dedupe_key,
                q_base.status, q_base.attempts, q_base.max_attempts, q_base.locked_by, q_base.locked_at,
                q_base.lease_expires_at, q_base.next_run_at, q_base.last_error, q_base.chapter_sort_key;
    \`;
`;

// regex to replace from "const disallowedChapterKeys = Array.from(this.inFlightChapterKeys);" to "const targetPool = client?.connect ? client : this.pool;"

const startStr = "const disallowedChapterKeys = Array.from(this.inFlightChapterKeys);";
const endStr = "const targetPool = client?.connect ? client : this.pool;";

const startIndex = code.indexOf(startStr);
const endIndex = code.indexOf(endStr);

if (startIndex > -1 && endIndex > -1) {
  const newCode = code.slice(0, startIndex) + claimSingleJobReplacement + "\n      " + code.slice(endIndex);
  fs.writeFileSync('src/core/scheduler/work-affinity-scheduler.ts', newCode);
  console.log("Patched WorkAffinityScheduler.ts");
} else {
  console.log("Could not find boundaries");
}
