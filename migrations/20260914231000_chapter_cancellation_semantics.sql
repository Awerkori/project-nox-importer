-- Migration: 20260914231000_chapter_cancellation_semantics.sql
-- Description: RPC to clearly separate temporary cancellation from permanent skip/gap for chapters.

CREATE OR REPLACE FUNCTION public.importer_cancel_chapter(
  p_job_id uuid,
  p_permanent_skip boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_job record;
  v_work_id uuid;
  v_sort_key numeric;
BEGIN
  -- 1. Fetch the queue job
  SELECT id, status, payload, chapter_sort_key, task_type
  INTO v_job
  FROM public.importer_queue
  WHERE id = p_job_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object('success', false, 'message', 'Job não encontrado na fila.');
  END IF;

  IF v_job.task_type != 'IMPORT_CHAPTER' THEN
    RETURN jsonb_build_object('success', false, 'message', 'Esta ação é apenas para capítulos.');
  END IF;

  v_work_id := (v_job.payload->>'workId')::uuid;
  v_sort_key := coalesce(v_job.chapter_sort_key, (v_job.payload->>'chapterNumber')::numeric);

  -- 2. Update the queue job status
  UPDATE public.importer_queue
  SET status = 'CANCELLED_BY_STAFF',
      cancel_reason = CASE WHEN p_permanent_skip THEN 'INTENTIONAL_GAP' ELSE 'TEMPORARY_PAUSE' END,
      updated_at = now()
  WHERE id = p_job_id;

  -- 3. Update the chapter mapping if permanent skip
  IF p_permanent_skip AND v_work_id IS NOT NULL AND v_sort_key IS NOT NULL THEN
    UPDATE public.importer_chapter_mappings
    SET is_gap = true,
        updated_at = now()
    WHERE work_id = v_work_id
      AND chapter_sort_key = v_sort_key;
  END IF;

  RETURN jsonb_build_object('success', true, 'message', CASE WHEN p_permanent_skip THEN 'Capítulo ignorado permanentemente (Gap Intencional).' ELSE 'Processamento do capítulo cancelado temporariamente.' END);
END;
$$;

REVOKE ALL ON FUNCTION public.importer_cancel_chapter(uuid, boolean) FROM public, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.importer_cancel_chapter(uuid, boolean) TO authenticated;
