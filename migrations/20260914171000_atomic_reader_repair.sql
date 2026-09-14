CREATE OR REPLACE FUNCTION public.importer_replace_pages(p_chapter_id uuid, p_pages jsonb)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE expected integer; valid integer;
BEGIN
 IF jsonb_typeof(p_pages) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'Invalid page manifest'; END IF;
 expected := jsonb_array_length(p_pages);
 IF expected<1 OR expected>1000 THEN RAISE EXCEPTION 'Invalid page count'; END IF;
 PERFORM 1 FROM chapters WHERE id=p_chapter_id AND origin='IMPORTER' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Importer chapter not found'; END IF;
 SELECT count(*) INTO valid FROM jsonb_array_elements(p_pages) x
 JOIN media m ON m.id=(x->>'media_id')::uuid
 WHERE m.storage_ready IS TRUE AND m.status IS DISTINCT FROM 'DELETED'
 AND m.bytes>0 AND m.mime LIKE 'image/%' AND m.width>0 AND m.height>0;
 IF valid<>expected THEN RAISE EXCEPTION 'Incomplete stored page manifest'; END IF;
 INSERT INTO pages(chapter_id,position,media_id,width,height)
 SELECT p_chapter_id,ordinality::integer,m.id,m.width,m.height
 FROM jsonb_array_elements(p_pages) WITH ORDINALITY x(item,ordinality)
 JOIN media m ON m.id=(x.item->>'media_id')::uuid
 ON CONFLICT(chapter_id,position) DO UPDATE SET media_id=excluded.media_id,width=excluded.width,height=excluded.height;
 DELETE FROM pages WHERE chapter_id=p_chapter_id AND position>expected;
 RETURN expected;
END;
$$;
REVOKE ALL ON FUNCTION public.importer_replace_pages(uuid,jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.importer_replace_pages(uuid,jsonb) TO service_role;
