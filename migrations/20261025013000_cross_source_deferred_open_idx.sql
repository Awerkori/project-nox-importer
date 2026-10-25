-- Cross-source deferred recheck only admits rows with deferredRounds < 2.
-- The older importer_lite_tasks_cross_source_deferred_idx also contains the
-- ~9.7k exhausted rows (deferredRounds = 2) that are due forever, so each
-- refill walked all of them before finding an open row (7.7 s mean, ~10% of
-- YSQL CPU in production ASH, 2026-10-25). This narrower partial index keeps
-- only rows that can still be re-admitted. Additive and online; the old index
-- is intentionally left in place (no destructive change).
CREATE INDEX IF NOT EXISTS importer_lite_tasks_cross_source_deferred_open_idx
  ON public.importer_lite_tasks (next_run_at ASC, id ASC)
  WHERE task_type = 'CATALOG' AND state = 'DONE'
    AND payload ? 'metadataCrossSourceDiscovery'
    AND (checkpoint ->> 'coverage') = 'CROSS_SOURCE_PARTIAL_EXTERNAL_DEFERRED'
    AND COALESCE(NULLIF(checkpoint ->> 'deferredRounds', '')::int, 0) < 2;
