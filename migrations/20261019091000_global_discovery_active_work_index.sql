BEGIN;

-- Global multifonte discovery asks whether a published canonical work has an
-- operational mapping. The V5 source cursor index starts with `source`, so it
-- cannot satisfy this inverse lookup without scanning every provider.
CREATE INDEX IF NOT EXISTS importer_work_mappings_v5_active_work_idx
  ON public.importer_work_mappings (work_id, source)
  WHERE work_id IS NOT NULL
    AND sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE');

COMMIT;
