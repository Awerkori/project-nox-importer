-- ==============================================================================
-- Project Nox Importer - Migration 20260913005500:
-- Fix publication barrier deadlock caused by multi-source chapter mappings.
--
-- Root Cause:
-- When a chapter is imported from multiple sources (cross-provider rescue / sync),
-- one mapping is chosen as page provider and publishes to public.chapters, while
-- duplicate/superseded mappings remain with chapter_id = NULL.
-- The previous barrier query did a LEFT JOIN public.chapters c ON c.id = m.chapter_id
-- and checked (c.published_at IS NULL OR c.id IS NULL).
-- Because superseded mappings have m.chapter_id = NULL, c.id IS NULL evaluated to TRUE,
-- falsely marking already-published chapters as blocking predecessors forever.
--
-- Definitive Fix:
-- 1. Check whether a chapter for the same work and chapter number is ACTUALLY published
--    in public.chapters (NOT EXISTS (SELECT 1 FROM public.chapters c WHERE c.work_id = p_work_id
--    AND (c.id = m.chapter_id OR c.number = coalesce(m.chapter_sort_key, m.chapter_number))
--    AND c.published_at IS NOT NULL)).
-- 2. Apply identical check to importer_queue to avoid blocking on redundant queued tasks
--    for chapters already published.
-- ==============================================================================

create or replace function public.importer_check_publication_barrier(
  p_work_id uuid,
  p_target_sort_key numeric
)
returns table (
  can_publish boolean,
  reason text,
  blocking_count integer,
  blocking_sort_keys numeric[]
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_sync_pending integer;
  v_blocking_keys numeric[];
  v_is_already_synced boolean;
begin
  -- Check if the work has already completed initial sync
  select exists (
    select 1
    from public.importer_work_mappings wm
    where wm.work_id = p_work_id
      and wm.sync_status = 'SYNCED'
      and wm.last_synced_at is not null
  ) into v_is_already_synced;

  -- Step 1: Safeguard 1 - Verify discovery is complete for this work
  -- If work has NEVER completed initial sync, any active SYNC_WORK blocks publication.
  if not v_is_already_synced then
    select count(*) into v_sync_pending
    from public.importer_queue q
    where q.task_type = 'SYNC_WORK'
      and q.status in ('QUEUED', 'IMPORTING')
      and (
        (q.payload->>'workId')::text = p_work_id::text
        or q.payload->>'sourceWorkId' in (
          select wm.source_work_id from public.importer_work_mappings wm where wm.work_id = p_work_id
        )
      );

    if v_sync_pending > 0 then
      return query select false, 'DISCOVERY_IN_PROGRESS'::text, v_sync_pending, array[]::numeric[];
      return;
    end if;
  end if;

  -- Step 2: Check for any preceding chapters in mappings or active queue that are NOT published
  select array_agg(distinct k order by k asc)
  into v_blocking_keys
  from (
    -- Preceding chapters in mappings that are NOT published in public.chapters
    select coalesce(m.chapter_sort_key, m.chapter_number) as k
    from public.importer_chapter_mappings m
    where m.work_id = p_work_id
      and coalesce(m.chapter_sort_key, m.chapter_number) < p_target_sort_key
      and m.is_gap = false
      and not exists (
        select 1 from public.chapters c
        where c.work_id = p_work_id
          and (c.id = m.chapter_id or c.number = coalesce(m.chapter_sort_key, m.chapter_number))
          and c.published_at is not null
      )

    union

    -- Preceding chapters active in queue that are NOT published in public.chapters
    select coalesce(q.chapter_sort_key, (q.payload->>'chapterNumber')::numeric) as k
    from public.importer_queue q
    where q.task_type = 'IMPORT_CHAPTER'
      and q.status in ('QUEUED', 'RETRY', 'IMPORTING')
      and (q.payload->>'workId')::text = p_work_id::text
      and coalesce(q.chapter_sort_key, (q.payload->>'chapterNumber')::numeric) < p_target_sort_key
      and not exists (
        select 1 from public.chapters c
        where c.work_id = p_work_id
          and c.number = coalesce(q.chapter_sort_key, (q.payload->>'chapterNumber')::numeric)
          and c.published_at is not null
      )
  ) sub;

  if v_blocking_keys is not null and array_length(v_blocking_keys, 1) > 0 then
    return query select false, 'PRECEDING_CHAPTERS_UNPUBLISHED'::text, array_length(v_blocking_keys, 1), v_blocking_keys;
    return;
  end if;

  -- Barrier cleared: all preceding chapters are published or confirmed gaps
  return query select true, 'OK'::text, 0, array[]::numeric[];
end;
$$;

revoke all on function public.importer_check_publication_barrier(uuid, numeric) from public, anon, authenticated;
grant execute on function public.importer_check_publication_barrier(uuid, numeric) to service_role;
