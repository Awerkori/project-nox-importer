-- Manifest-proven P1 repair is a bounded producer.  Its source rows must be
-- found without scanning the full historical manifest while chapter workers
-- share the small direct-YSQL pool.
--
-- This is additive only: it neither changes chapter identities nor creates
-- tasks by itself.  The runtime still requires positive `confirmed` and
-- `has_chapter` evidence before it can enqueue a repair.
CREATE INDEX IF NOT EXISTS importer_lite_chapter_sources_manifest_repair_idx
  ON public.importer_lite_chapter_sources
    (updated_at, work_id, canonical_chapter_key, source_rank, source)
  WHERE confirmed=true AND has_chapter=true;
