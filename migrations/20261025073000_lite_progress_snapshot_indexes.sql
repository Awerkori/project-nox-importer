-- The operational heartbeat reads these four values every minute whenever
-- the chapter lane is quiet. Without narrow predicates, the two minima below
-- scan the READY backlog and the full historical task table respectively.
-- These indexes are additive and serve telemetry only; they do not affect
-- claims, ordering, priorities, publication, or task transitions.

CREATE INDEX IF NOT EXISTS importer_lite_tasks_progress_ready_created_idx
  ON public.importer_lite_tasks (created_at ASC)
  WHERE state = 'READY'
    AND monitoring = false;

CREATE INDEX IF NOT EXISTS importer_lite_tasks_progress_monitor_next_run_idx
  ON public.importer_lite_tasks (next_run_at ASC)
  WHERE task_type = 'CATALOG'
    AND monitoring = true
    AND state IN ('READY', 'RETRY_WAIT');
