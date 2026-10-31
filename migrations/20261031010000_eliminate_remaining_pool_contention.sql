-- Eliminates remaining YugabyteDB connection-pool contention on importer_lite_ready_works
-- and importer_lite_tasks:
-- 1. Replaces tombstone-bloated legacy ready_works indexes and adds an operation_rank <= 1
--    partial index so readyImportSourceRows executes an ordered index scan without Sort.
-- 2. Adds partial claim/lookup indexes for metadataEnrichment replay and v2/v3 producer
--    queries so claimStoredMetadataReplay and refillStoredMetadataEnrichment never fall
--    back to sequential scans of importer_lite_tasks.

CREATE INDEX IF NOT EXISTS importer_lite_ready_works_claim_rank_v2_idx
  ON public.importer_lite_ready_works (priority ASC, operation_rank ASC, next_run_at ASC, last_claimed_at ASC);

CREATE INDEX IF NOT EXISTS importer_lite_ready_works_fair_claim_v2_idx
  ON public.importer_lite_ready_works (priority ASC, operation_rank ASC, last_claimed_at ASC NULLS FIRST, next_run_at ASC, work_key ASC);

CREATE INDEX IF NOT EXISTS importer_lite_ready_works_op1_active_idx
  ON public.importer_lite_ready_works (priority ASC, last_claimed_at ASC NULLS FIRST, next_run_at ASC, work_key ASC)
  WHERE (operation_rank <= 1);

CREATE INDEX IF NOT EXISTS importer_lite_tasks_metadata_replay_claim_idx
  ON public.importer_lite_tasks (priority ASC, next_run_at ASC, created_at ASC, id ASC)
  WHERE ((task_type = 'CATALOG') AND (monitoring = false) AND (state IN ('READY', 'RETRY_WAIT')) AND (payload ? 'metadataEnrichment'));

CREATE INDEX IF NOT EXISTS importer_lite_tasks_v23_active_idx
  ON public.importer_lite_tasks (next_run_at ASC, id ASC)
  WHERE ((task_type = 'CATALOG') AND (state IN ('READY', 'RUNNING', 'RETRY_WAIT')) AND ((payload ->> 'metadataEnrichmentVersion') IN ('2', '3')));

CREATE INDEX IF NOT EXISTS importer_lite_tasks_metadata_source_updated_idx
  ON public.importer_lite_tasks (((payload ->> 'source')) ASC, updated_at DESC)
  WHERE (payload ? 'metadataEnrichment');

DROP INDEX IF EXISTS public.importer_lite_ready_works_claim_idx;
DROP INDEX IF EXISTS public.importer_lite_ready_works_claim_rank_idx;
DROP INDEX IF EXISTS public.importer_lite_ready_works_fair_claim_idx;
