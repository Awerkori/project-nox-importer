-- Bound discovery admission counts without indexing unrelated chapter jobs.
CREATE INDEX IF NOT EXISTS idx_importer_queue_source_task_status
  ON public.importer_queue (source, task_type, status)
  WHERE task_type IN ('DISCOVER_WORKS', 'SYNC_WORK');
