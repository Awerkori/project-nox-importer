const fs = require('fs');

let code = fs.readFileSync('src/core/scheduler/work-affinity-scheduler.ts', 'utf8');

const claimStaffReplacement = `
    const allowedSourcesFilter = opts.allowedSources && opts.allowedSources.length > 0 ? \`AND q.source = ANY($1::text[])\` : \`\`;
    const disallowedWorkIdsFilter = opts.disallowedWorkIds && opts.disallowedWorkIds.length > 0 ? \`AND NOT ((q.payload->>'workId') = ANY($2::text[]))\` : \`\`;
    const disallowedChapterKeysFilter = disallowedChapterKeys.length > 0 ? \`AND NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($3::text[]))\` : \`\`;
    const staffWorkIdsFilter = staffWorkIds && staffWorkIds.length > 0 ? \`OR (payload->>'workId') = ANY($6::text[])\` : \`\`;

    const query = \`
      WITH q_candidates AS (
        SELECT id, payload, source, chapter_sort_key, next_run_at, priority
        FROM importer_queue
        WHERE (status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW()))
          AND task_type = 'IMPORT_CHAPTER'
          AND attempts < COALESCE(max_attempts, 7)
          AND (
            priority >= 1000
            \${staffWorkIdsFilter}
          )
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
        LEFT JOIN importer_staff_requests sr 
          ON sr.work_id = (q.payload->>'workId')::uuid
         AND sr.status IN ('ACTIVE', 'QUEUED', 'IMPORTING', 'RETRYING')
        WHERE s.enabled = true
          AND \${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          \${CANONICAL_PUBLISHED_CLAIM_FILTER}
          \${CANONICAL_FRONTIER_CLAIM_FILTER}
          \${allowedSourcesFilter}
          \${disallowedWorkIdsFilter}
          \${disallowedChapterKeysFilter}
          \${CANONICAL_ACTIVE_CLAIM_FILTER}
        ORDER BY
          CASE WHEN sr.work_id IS NOT NULL THEN 0 ELSE 1 END,
          COALESCE(array_position($6::text[], q.payload->>'workId'), 2147483647),
          q.priority DESC, 
          q.chapter_sort_key ASC NULLS LAST, 
          q.next_run_at ASC
        FOR UPDATE OF q_base SKIP LOCKED
        LIMIT 1
      )
      UPDATE importer_queue q_base
      SET status = 'IMPORTING',
          locked_by = $4,
          locked_at = NOW(),
          lease_expires_at = NOW() + ($5::text || ' minutes')::interval,
          attempts = q_base.attempts + 1,
          updated_at = NOW()
      FROM to_lock
      WHERE q_base.id = to_lock.id
      RETURNING q_base.id, q_base.task_type, q_base.source, q_base.priority, q_base.payload, q_base.dedupe_key,
                q_base.status, q_base.attempts, q_base.max_attempts, q_base.locked_by, q_base.locked_at,
                q_base.lease_expires_at, q_base.next_run_at, q_base.last_error, q_base.chapter_sort_key;
    \`;
`;

const searchPoint2 = code.indexOf("private async claimStaffForcedJob");
const qStart1 = "    const query = `\n      WITH q_candidates AS (";
const startIndex2 = code.indexOf(qStart1, searchPoint2);
const endIndexStr = "q_base.lease_expires_at, q_base.next_run_at, q_base.last_error, q_base.chapter_sort_key;\n    `;";
const endIndex2 = code.indexOf(endIndexStr, startIndex2);

if (startIndex2 > -1 && endIndex2 > -1) {
  code = code.slice(0, startIndex2) + claimStaffReplacement.trim() + code.slice(endIndex2 + endIndexStr.length);
  console.log("Patched claimStaffForcedJob");
  fs.writeFileSync('src/core/scheduler/work-affinity-scheduler.ts', code);
} else {
  console.log("Not found", startIndex2, endIndex2);
}
