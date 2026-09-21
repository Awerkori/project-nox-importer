-- Migration: 20260921133000_fix_releases_and_triggers.sql
ALTER TABLE public.works ADD COLUMN IF NOT EXISTS latest_chapter_published_at timestamptz;

-- 1. Index on chapters (published_at DESC, id DESC) for fast releases queries
CREATE INDEX IF NOT EXISTS idx_chapters_published_at ON public.chapters (published_at DESC, id DESC);

-- 2. Update trigger function update_work_latest_chapter()
-- Any published chapter (whether fresh release or backfill) updates works.latest_chapter_published_at
CREATE OR REPLACE FUNCTION public.update_work_latest_chapter()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  IF (TG_OP = 'INSERT') THEN
    IF NEW.published_at IS NOT NULL THEN
      UPDATE public.works
      SET latest_chapter_published_at = CASE 
            WHEN latest_chapter_published_at IS NULL THEN NEW.published_at
            ELSE GREATEST(latest_chapter_published_at, NEW.published_at)
          END
      WHERE id = NEW.work_id;
    END IF;
  ELSIF (TG_OP = 'UPDATE') THEN
    IF NEW.published_at IS DISTINCT FROM OLD.published_at THEN
      IF NEW.published_at IS NOT NULL THEN
        UPDATE public.works
        SET latest_chapter_published_at = CASE 
              WHEN latest_chapter_published_at IS NULL THEN NEW.published_at
              ELSE GREATEST(latest_chapter_published_at, NEW.published_at)
            END
        WHERE id = NEW.work_id;
      ELSIF NEW.published_at IS NULL THEN
        UPDATE public.works
        SET latest_chapter_published_at = (
          SELECT MAX(published_at)
          FROM public.chapters
          WHERE work_id = NEW.work_id AND published_at IS NOT NULL
        )
        WHERE id = NEW.work_id;
      END IF;
    END IF;
  ELSIF (TG_OP = 'DELETE') THEN
    IF OLD.published_at IS NOT NULL THEN
      UPDATE public.works
      SET latest_chapter_published_at = (
        SELECT MAX(published_at)
        FROM public.chapters
        WHERE work_id = OLD.work_id AND published_at IS NOT NULL
      )
      WHERE id = OLD.work_id;
    END IF;
  END IF;
  RETURN NULL;
END;
$function$;

-- 3. One-time sync for any works whose latest_chapter_published_at is out of sync
UPDATE public.works w
SET latest_chapter_published_at = sub.max_pub
FROM (
  SELECT work_id, MAX(published_at) AS max_pub
  FROM public.chapters
  WHERE published_at IS NOT NULL
  GROUP BY work_id
) sub
WHERE w.id = sub.work_id
  AND (w.latest_chapter_published_at IS DISTINCT FROM sub.max_pub);

-- 4. Definitive get_recent_releases RPC
-- Returns works ordered by latest_chapter_published_at DESC, with all their published chapters up to p_chapters_per_work (default 50)
CREATE OR REPLACE FUNCTION public.get_recent_releases(
  p_limit integer DEFAULT 15,
  p_chapters_per_work integer DEFAULT 50,
  p_kind text DEFAULT NULL
)
RETURNS TABLE (
  work_id uuid,
  work_slug text,
  work_title text,
  work_kind text,
  work_cover_id uuid,
  work_content_rating text,
  latest_published_at timestamptz,
  chapter_id uuid,
  chapter_number numeric,
  chapter_title text,
  chapter_published_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $function$
  WITH recent_works AS (
    SELECT
      w.id AS work_id,
      w.latest_chapter_published_at
    FROM public.works w
    WHERE w.published = true
      AND w.latest_chapter_published_at IS NOT NULL
      AND (p_kind IS NULL OR p_kind = 'ALL' OR w.kind = p_kind)
    ORDER BY w.latest_chapter_published_at DESC, w.id DESC
    LIMIT p_limit
  ),
  ranked_chapters AS (
    SELECT
      c.id AS chapter_id,
      c.work_id,
      c.number AS chapter_number,
      c.title AS chapter_title,
      c.published_at AS chapter_published_at,
      ROW_NUMBER() OVER (PARTITION BY c.work_id ORDER BY c.published_at DESC, c.number DESC) AS rn
    FROM public.chapters c
    INNER JOIN recent_works rw ON rw.work_id = c.work_id
    WHERE c.published_at IS NOT NULL
  )
  SELECT
    rw.work_id,
    w.slug AS work_slug,
    w.title AS work_title,
    w.kind AS work_kind,
    w.cover_id AS work_cover_id,
    w.content_rating AS work_content_rating,
    rw.latest_chapter_published_at,
    rc.chapter_id,
    rc.chapter_number,
    rc.chapter_title,
    rc.chapter_published_at
  FROM recent_works rw
  INNER JOIN public.works w ON w.id = rw.work_id
  INNER JOIN ranked_chapters rc ON rc.work_id = rw.work_id AND rc.rn <= p_chapters_per_work
  ORDER BY rw.latest_chapter_published_at DESC, rw.work_id, rc.chapter_published_at DESC, rc.chapter_number DESC;
$function$;

GRANT EXECUTE ON FUNCTION public.get_recent_releases(integer, integer, text) TO public;
