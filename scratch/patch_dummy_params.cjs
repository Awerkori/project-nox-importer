const fs = require('fs');

let code = fs.readFileSync('src/core/scheduler/work-affinity-scheduler.ts', 'utf8');

// 1. executeClaimCatalogQuery
code = code.replace(
  'WITH eligible_sources AS MATERIALIZED (',
  'WITH dummy_params AS (SELECT $1::text[], $2::text[], $3::text[], $4::text, $5::int, $6::int, $7::int),\n      eligible_sources AS MATERIALIZED ('
);

// 2. claimStaffForcedJob
code = code.replace(
  'WITH q_candidates AS (\n        SELECT id, payload, source, chapter_sort_key, next_run_at, priority\n        FROM importer_queue\n        WHERE (status = \'QUEUED\' OR (status = \'RETRY\' AND next_run_at <= NOW()))\n          AND task_type = \'IMPORT_CHAPTER\'\n          AND attempts < COALESCE(max_attempts, 7)\n          AND (payload->>\'workId\') = ANY($6::text[])',
  'WITH dummy_params AS (SELECT $1::text[], $2::text[], $3::text[], $4::text, $5::int, $6::text[]),\n      q_candidates AS (\n        SELECT id, payload, source, chapter_sort_key, next_run_at, priority\n        FROM importer_queue\n        WHERE (status = \'QUEUED\' OR (status = \'RETRY\' AND next_run_at <= NOW()))\n          AND task_type = \'IMPORT_CHAPTER\'\n          AND attempts < COALESCE(max_attempts, 7)\n          AND (payload->>\'workId\') = ANY($6::text[])'
);

// 3. claimSingleJob
code = code.replace(
  'WITH q_candidates AS (\n        SELECT id, payload, source, chapter_sort_key, next_run_at, priority\n        FROM importer_queue\n        WHERE (status = \'QUEUED\' OR (status = \'RETRY\' AND next_run_at <= NOW()))\n          AND task_type = \'IMPORT_CHAPTER\'\n          AND attempts < COALESCE(max_attempts, 7)\n          ${minPriorityFilter}',
  'WITH dummy_params AS (SELECT $1::text[], $2::int, $3::text, $4::numeric, $5::text, $6::int, $7::text[], $8::text[], $9::text[], $10::int),\n      q_candidates AS (\n        SELECT id, payload, source, chapter_sort_key, next_run_at, priority\n        FROM importer_queue\n        WHERE (status = \'QUEUED\' OR (status = \'RETRY\' AND next_run_at <= NOW()))\n          AND task_type = \'IMPORT_CHAPTER\'\n          AND attempts < COALESCE(max_attempts, 7)\n          ${minPriorityFilter}'
);

fs.writeFileSync('src/core/scheduler/work-affinity-scheduler.ts', code);
console.log("Patched dummy_params");
