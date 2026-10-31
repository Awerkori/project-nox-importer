-- Persists the critical P0 monitoring index for fast claimMonitor queries
-- Replaces table scans and in-memory sorting with 1-row index seeks per active source.

CREATE INDEX IF NOT EXISTS importer_lite_tasks_p0_source_due_idx
ON public.importer_lite_tasks (((payload ->> 'source'::text)) ASC, priority ASC, next_run_at ASC)
WHERE ((state = 'READY'::text) AND (task_type = 'CATALOG'::text) AND (monitoring = true));
