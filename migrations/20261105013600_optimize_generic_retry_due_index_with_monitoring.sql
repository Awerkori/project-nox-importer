-- Keep generic retry recovery and monitor recovery in index order.  The
-- monitoring key lets both reconciler passes stop at their small LIMIT rather
-- than scanning unrelated retry work and sorting it.

DROP INDEX IF EXISTS public.importer_lite_tasks_generic_retry_due_idx;

CREATE INDEX importer_lite_tasks_generic_retry_due_idx
  ON public.importer_lite_tasks USING lsm (monitoring ASC, next_run_at ASC)
  WHERE ((state = 'RETRY_WAIT'::text)
    AND (COALESCE(reason, ''::text) <> 'SOURCE_RATE_LIMITED'::text));
