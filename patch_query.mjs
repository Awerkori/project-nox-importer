import fs from 'fs';
const f = 'src/core/scheduler/work-affinity-scheduler.ts';
let code = fs.readFileSync(f, 'utf8');

const target = `      WITH q_candidates AS (
        SELECT id, payload, source, chapter_sort_key, next_run_at, priority
        FROM importer_queue
        WHERE (status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW()))
          AND task_type = 'IMPORT_CHAPTER'
          AND attempts < COALESCE(max_attempts, 7)
          AND (
            priority >= 1000
            OR (payload->>'workId') = ANY($6::text[])
          )
      ),
      to_lock AS (
        SELECT q.id
        FROM importer_queue q
        JOIN q_candidates qc ON q.id = qc.id
        JOIN importer_sources s ON s.id = qc.source
        LEFT JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = (qc.payload->>'workId')::uuid
            AND c.published_at IS NOT NULL
        ) pub ON TRUE
        LEFT JOIN importer_staff_requests sr 
          ON sr.work_id = (qc.payload->>'workId')::uuid
         AND sr.status IN ('ACTIVE', 'QUEUED', 'IMPORTING', 'RETRYING')
        WHERE s.enabled = true
          AND \${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          \${CANONICAL_PUBLISHED_CLAIM_FILTER}
          \${CANONICAL_FRONTIER_CLAIM_FILTER}
          AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
          AND (
            q.priority >= 1000
            OR (q.payload->>'workId') = ANY($6::text[])
          )
          AND ($2::text[] IS NULL OR NOT ((q.payload->>'workId') = ANY($2::text[])))
          AND ($3::text[] IS NULL OR NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($3::text[])))
          \${CANONICAL_ACTIVE_CLAIM_FILTER}
        ORDER BY
          CASE WHEN sr.work_id IS NOT NULL THEN 0 ELSE 1 END,
          COALESCE(array_position($6::text[], q.payload->>'workId'), 2147483647),
          q.priority DESC, 
          q.chapter_sort_key ASC NULLS LAST, 
          q.next_run_at ASC
        FOR UPDATE OF q SKIP LOCKED
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
                q.lease_expires_at, q.next_run_at, q.last_error, q.chapter_sort_key;`;

const replacement = `      WITH q_candidates AS (
        SELECT id, payload, source, chapter_sort_key, next_run_at, priority
        FROM importer_queue
        WHERE (status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW()))
          AND task_type = 'IMPORT_CHAPTER'
          AND attempts < COALESCE(max_attempts, 7)
          AND (
            priority >= 1000
            OR (payload->>'workId') = ANY($6::text[])
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
          AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
          AND ($2::text[] IS NULL OR NOT ((q.payload->>'workId') = ANY($2::text[])))
          AND ($3::text[] IS NULL OR NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($3::text[])))
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
                q_base.lease_expires_at, q_base.next_run_at, q_base.last_error, q_base.chapter_sort_key;`;

if (!code.includes(target)) {
  console.log("Not found!");
  // log the substring that mismatches
} else {
  code = code.replace(target, replacement);
  fs.writeFileSync(f, code);
  console.log("Patched successfully");
}
