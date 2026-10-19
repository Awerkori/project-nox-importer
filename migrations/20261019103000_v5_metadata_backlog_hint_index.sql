BEGIN;

-- V5 uses this narrow, additive projection only to find one active source
-- with still-unchecked historical metadata when its fair cursor is sparse.
-- The runtime validates the mapping again before it creates any task.
CREATE INDEX IF NOT EXISTS importer_lite_metadata_enrichment_v5_backlog_hint_idx
  ON public.importer_lite_metadata_enrichment (updated_at, source, source_work_id)
  WHERE version < 5;

COMMIT;
