BEGIN;

-- Source metadata refresh claims active tasks and counts pending refetch tasks
-- at a high cadence. Without this narrow partial index, those queries scan the
-- unbounded task table. Because active refetch tasks are bounded by capacity (<= 24),
-- the indexed projection is virtually zero-cost while turning multi-second table
-- scans into sub-millisecond index scans.
CREATE INDEX IF NOT EXISTS importer_lite_tasks_source_refetch_active_idx
  ON public.importer_lite_tasks (next_run_at, id)
  WHERE task_type='CATALOG'
    AND state IN ('READY','RUNNING','RETRY_WAIT')
    AND payload ? 'metadataSourceRefresh';

COMMIT;
