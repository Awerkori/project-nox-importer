-- The 30-second P1 cooldown reconciler must select a fair work before it
-- examines that work's chapter routes. A generic RETRY_WAIT index forced it
-- to revisit every historical rate-limited task on each turn. This partial
-- work-ordered index contains only that recovery universe.
CREATE INDEX IF NOT EXISTS importer_lite_tasks_source_cooldown_work_idx
  ON public.importer_lite_tasks (work_key, next_run_at, created_at, id)
  WHERE state='RETRY_WAIT' AND task_type='IMPORT_CHAPTER'
    AND monitoring=false AND reason='SOURCE_RATE_LIMITED';
