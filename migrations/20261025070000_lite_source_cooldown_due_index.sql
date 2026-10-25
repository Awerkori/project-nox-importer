-- The source-cooldown reconciler selects only due RETRY_WAIT chapter tasks.
-- Its old work-first projection is useful for fairness after selection, but
-- cannot bound the initial `next_run_at <= now()` lookup. On production this
-- made YSQL scan the generic retry index across the whole task table before
-- reaching a rate-limited route, competing with chapter finalization.
--
-- This additive partial index exactly matches the durable eligibility fence;
-- the reconciler still applies work/source fairness and never changes chapter
-- ordering, retry policy, or source health semantics.
CREATE INDEX IF NOT EXISTS importer_lite_tasks_source_cooldown_due_idx
  ON public.importer_lite_tasks (next_run_at ASC, work_key ASC, created_at ASC, id ASC)
  WHERE state = 'RETRY_WAIT'
    AND task_type = 'IMPORT_CHAPTER'
    AND monitoring = false
    AND reason = 'SOURCE_RATE_LIMITED';
