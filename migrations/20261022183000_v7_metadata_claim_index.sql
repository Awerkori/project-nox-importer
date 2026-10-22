BEGIN;

-- V7 cross-source metadata discovery partial claim index.
-- Drops claim query execution time from ~3900ms to <5ms by indexing
-- only active CATALOG tasks under the V7 idempotency prefix, completely
-- bypassing the 5400+ completed DONE rows.
CREATE INDEX IF NOT EXISTS importer_lite_tasks_v7_claim_idx
  ON public.importer_lite_tasks (next_run_at ASC, id ASC)
  WHERE task_type = 'CATALOG'
    AND state = ANY (ARRAY['READY', 'RUNNING', 'RETRY_WAIT'])
    AND idempotency_key >= 'lite:metadata:cross-source:v7:'
    AND idempotency_key < 'lite:metadata:cross-source:v7:~';

COMMIT;
