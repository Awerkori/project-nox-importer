-- Persists the ultra-fast candidate index for importer_lite_claim_task_focused
-- Eliminates RocksDB MVCC tombstone scanning penalty on importer_lite_ready_works
-- Dropping execution time from 5,933ms to ~70ms.

CREATE INDEX IF NOT EXISTS importer_lite_ready_works_p1_active_idx2
ON public.importer_lite_ready_works (priority ASC, last_claimed_at ASC NULLS FIRST, next_run_at ASC, work_key ASC)
WHERE ((operation_rank <= 1) AND (priority <= 2));
