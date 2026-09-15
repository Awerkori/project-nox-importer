import fs from 'fs';

const p = '../project-nox-manga/supabase/migrations/20260915020000_perfect_gap_and_fairness.sql';
let sql = fs.readFileSync(p, 'utf8');

// Replace RETURNING id, ...
const returningOld = "RETURNING id, task_type, source, priority, payload, dedupe_key, status, attempts, max_attempts, locked_by, locked_at, lease_expires_at, next_run_at, last_error, chapter_sort_key;";
const returningNew = "RETURNING public.importer_queue.id, public.importer_queue.task_type, public.importer_queue.source, public.importer_queue.priority, public.importer_queue.payload, public.importer_queue.dedupe_key, public.importer_queue.status, public.importer_queue.attempts, public.importer_queue.max_attempts, public.importer_queue.locked_by, public.importer_queue.locked_at, public.importer_queue.lease_expires_at, public.importer_queue.next_run_at, public.importer_queue.last_error, public.importer_queue.chapter_sort_key;";

sql = sql.replace(returningOld, returningNew);

// Replace WHERE id = v_focus_request_id;
const whereOld = "WHERE id = v_focus_request_id;";
const whereNew = "WHERE public.importer_staff_requests.id = v_focus_request_id;";

sql = sql.replace(whereOld, whereNew);

fs.writeFileSync(p, sql);
console.log("Patched ambiguous IDs!");
