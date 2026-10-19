BEGIN;

-- Cross-source global alias discovery claims active tasks and counts pending
-- lane allocations at a high cadence. Without these narrow partial indexes,
-- those queries scan the unbounded task table. Because active discovery tasks
-- are capped at capacity (<= 2), the indexed projection is virtually zero-cost
-- while turning a multi-second table scan into a sub-millisecond index scan.
CREATE INDEX IF NOT EXISTS importer_lite_tasks_cross_source_active_idx
  ON public.importer_lite_tasks (next_run_at, id)
  WHERE task_type='CATALOG'
    AND state IN ('READY','RUNNING','RETRY_WAIT')
    AND payload ? 'metadataCrossSourceDiscovery';

CREATE INDEX IF NOT EXISTS importer_lite_tasks_cross_source_deferred_idx
  ON public.importer_lite_tasks (next_run_at, id)
  WHERE task_type='CATALOG' AND state='DONE'
    AND payload ? 'metadataCrossSourceDiscovery'
    AND checkpoint->>'coverage'='CROSS_SOURCE_PARTIAL_EXTERNAL_DEFERRED';

COMMIT;
