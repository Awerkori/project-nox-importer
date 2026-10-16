-- The dedicated historical metadata claimant only ever considers active P2
-- refetch tasks. Its former source/history index also contains every DONE
-- task, so an empty or one-row claim scanned remote historical rows before
-- establishing that no runnable task existed. Keep the candidate projection
-- tiny and ordered exactly as importer_lite_claim_source_metadata_task reads
-- it. Production applies the equivalent CREATE INDEX CONCURRENTLY command;
-- this transactional form is retained for the disposable migration fixture.
CREATE INDEX IF NOT EXISTS importer_lite_metadata_refetch_claim_idx
  ON public.importer_lite_tasks (next_run_at, created_at, id)
  WHERE task_type='CATALOG' AND monitoring=false
    AND state IN ('READY','RETRY_WAIT')
    AND payload ? 'metadataSourceRefresh';
