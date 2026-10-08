import fs from 'fs';
const f = 'src/core/scheduler/work-affinity-scheduler.ts';
let code = fs.readFileSync(f, 'utf8');

const target = `      WITH to_lock AS (
        SELECT q.id
        FROM importer_queue q
        JOIN importer_sources s ON s.id = q.source
        LEFT JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = (q.payload->>'workId')::uuid
            AND c.published_at IS NOT NULL
        ) pub ON TRUE
        WHERE (
          q.status = 'QUEUED'
          OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
        )
          AND q.task_type = 'IMPORT_CHAPTER'
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND s.enabled = true
          AND \${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          \${CANONICAL_PUBLISHED_CLAIM_FILTER}
          \${CANONICAL_FRONTIER_CLAIM_FILTER}
          AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
          AND ($2::int IS NULL OR q.priority >= $2::int)
          AND ($10::int IS NULL OR q.priority <= $10::int)
          AND ($3::text IS NULL OR (q.payload->>'workId') = $3::text)
          AND ($4::numeric IS NULL OR q.chapter_sort_key = $4::numeric)
          AND ($7::text[] IS NULL OR (q.payload->>'workId') = ANY($7::text[]))
          AND ($8::text[] IS NULL OR NOT ((q.payload->>'workId') = ANY($8::text[])))
          AND ($9::text[] IS NULL OR NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($9::text[])))
          \${CANONICAL_ACTIVE_CLAIM_FILTER}
        \${orderClause}
        FOR UPDATE OF q SKIP LOCKED
        LIMIT 1
      )
      UPDATE importer_queue q
      SET status = 'IMPORTING',
          locked_by = $5,
          locked_at = NOW(),
          lease_expires_at = NOW() + ($6::text || ' minutes')::interval,
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
          AND ($2::int IS NULL OR priority >= $2::int)
          AND ($10::int IS NULL OR priority <= $10::int)
          AND ($3::text IS NULL OR (payload->>'workId') = $3::text)
          AND ($4::numeric IS NULL OR chapter_sort_key = $4::numeric)
          AND ($7::text[] IS NULL OR (payload->>'workId') = ANY($7::text[]))
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
          AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
          AND ($8::text[] IS NULL OR NOT ((q.payload->>'workId') = ANY($8::text[])))
          AND ($9::text[] IS NULL OR NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($9::text[])))
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
                q_base.lease_expires_at, q_base.next_run_at, q_base.last_error, q_base.chapter_sort_key;`;

if (!code.includes(target)) {
  console.log("Not found fallback!");
} else {
  code = code.replace(target, replacement);
  fs.writeFileSync(f, code);
  console.log("Patched fallback successfully");
}
