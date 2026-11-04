-- The cooldown reconciler needs these projected columns after its ordered
-- SOURCE_RATE_LIMITED lookup.  The older partial key-only index still forced
-- YSQL to read table rows and sort under a large overdue backlog.
CREATE INDEX IF NOT EXISTS importer_lite_tasks_source_cooldown_due_cover_idx
  ON public.importer_lite_tasks (next_run_at ASC, work_key ASC, created_at ASC, id ASC)
  INCLUDE (work_id, chapter_key, payload, updated_at)
  WHERE state = 'RETRY_WAIT'
    AND monitoring = false
    AND task_type = 'IMPORT_CHAPTER'
    AND reason = 'SOURCE_RATE_LIMITED';
