BEGIN;

-- V5 walks a small, rotating source window.  The legacy source/work-id
-- unique key can locate a provider but still reads inactive historical rows
-- and then fetches the canonical work id remotely.  Keep that source-backed
-- alias pass on a narrow active-mapping projection instead.
CREATE INDEX IF NOT EXISTS importer_work_mappings_v5_active_source_work_idx
  ON public.importer_work_mappings (source, source_work_id, work_id)
  WHERE work_id IS NOT NULL
    AND sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE');

COMMIT;
