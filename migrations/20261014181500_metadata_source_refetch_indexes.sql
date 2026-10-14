BEGIN;

-- Keep source fairness admission bounded as the task table grows. The index
-- contains only metadata refetch tasks, which are expected to be a tiny P2
-- subset of the chapter queue.
CREATE INDEX IF NOT EXISTS importer_lite_metadata_refetch_source_created_idx
  ON public.importer_lite_tasks ((payload->>'source'), created_at DESC)
  WHERE task_type='CATALOG' AND payload ? 'metadataSourceRefresh';

COMMIT;
