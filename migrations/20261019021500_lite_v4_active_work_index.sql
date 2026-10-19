-- V4's exclusive historical metadata lane may hold two distinct canonical
-- works in flight.  Selecting a successor excludes those work ids. Without
-- this narrow partial index, that anti-join scans the whole task history on
-- every producer turn, even though only a couple of active V4 rows exist.
--
-- Additive only: it does not change task state, aliases, tags, or claim
-- priority. The production runner applies the same idempotent statement with
-- a bounded lock timeout because YugabyteDB has no concurrent index build.
CREATE INDEX IF NOT EXISTS importer_lite_tasks_v4_active_work_idx
  ON public.importer_lite_tasks (work_id)
  WHERE task_type='CATALOG'
    AND state IN ('READY','RUNNING','RETRY_WAIT')
    AND payload->>'metadataEnrichmentVersion'='4';
