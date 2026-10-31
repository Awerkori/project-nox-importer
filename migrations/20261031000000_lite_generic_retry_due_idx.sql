-- Eliminates 13,000+ row index scan + sort on every 10s reconcile() turn
-- when large SOURCE_RATE_LIMITED retry backlogs exist, keeping generic crash
-- and transient-error retry recovery O(due generic retries).

CREATE INDEX IF NOT EXISTS importer_lite_tasks_generic_retry_due_idx
ON public.importer_lite_tasks (next_run_at ASC)
WHERE state = 'RETRY_WAIT' AND COALESCE(reason, '') <> 'SOURCE_RATE_LIMITED';
