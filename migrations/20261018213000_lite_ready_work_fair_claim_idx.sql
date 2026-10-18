-- Claims select one runnable work by priority, operation rank and oldest
-- claim.  The prior index placed next_run_at before last_claimed_at, while
-- the function deliberately orders those two columns in the opposite order
-- to preserve work-level fairness.  On a deep P1 backlog that mismatch made
-- every claim sort a remote ready-work set before it could lock one row.
--
-- This is additive only: it changes neither priority policy nor task state.
-- Production applies the equivalent CREATE INDEX CONCURRENTLY statement via
-- scripts/apply-lite-claim-performance-migration.ts; this transactional form
-- keeps the disposable PGlite integration fixture faithful to that schema.
CREATE INDEX IF NOT EXISTS importer_lite_ready_works_fair_claim_idx
  ON public.importer_lite_ready_works
    (priority, operation_rank, last_claimed_at NULLS FIRST, next_run_at, work_key);
