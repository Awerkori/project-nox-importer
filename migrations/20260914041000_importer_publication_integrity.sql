-- Validate stored pages in the same transaction that changes published_at.
-- This gate is scoped to Importer chapters; editorial Scan Staff is independent.
CREATE OR REPLACE FUNCTION public.guard_importer_publication_integrity()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE n integer; distinct_positions integer; first_page integer; last_page integer; invalid integer; expected integer;
BEGIN
  IF NEW.origin IS DISTINCT FROM 'IMPORTER' OR NEW.published_at IS NULL THEN RETURN NEW; END IF;
  SELECT count(*),count(DISTINCT p.position),min(p.position),max(p.position),count(*) FILTER (
    WHERE m.id IS NULL OR m.storage_ready IS NOT TRUE OR m.status='DELETED'
      OR coalesce(m.bytes,0)<=0 OR coalesce(m.mime,'') NOT LIKE 'image/%'
      OR coalesce(m.width,0)<=0 OR coalesce(m.height,0)<=0
  ) INTO n,distinct_positions,first_page,last_page,invalid
  FROM public.pages p LEFT JOIN public.media m ON m.id=p.media_id WHERE p.chapter_id=NEW.id;
  IF n=0 OR distinct_positions<>n OR first_page<>1 OR last_page<>n OR invalid>0 THEN
    RAISE EXCEPTION 'IMPORTER_PUBLICATION_INTEGRITY: chapter %, pages %, invalid %',NEW.id,n,invalid USING ERRCODE='23514';
  END IF;
  SELECT page_count INTO expected FROM public.importer_chapter_mappings
    WHERE chapter_id=NEW.id AND is_page_provider AND status='STAGED'
    ORDER BY updated_at DESC LIMIT 1;
  IF expected IS NOT NULL AND expected<>n THEN
    RAISE EXCEPTION 'IMPORTER_PUBLICATION_INTEGRITY: expected %, stored %',expected,n USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS importer_publication_integrity ON public.chapters;
CREATE TRIGGER importer_publication_integrity BEFORE INSERT OR UPDATE OF published_at ON public.chapters
FOR EACH ROW EXECUTE FUNCTION public.guard_importer_publication_integrity();
