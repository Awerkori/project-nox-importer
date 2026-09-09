-- ==============================================================================
-- Project Nox Manga - Migration 20260909010000:
-- Importer Absolute Priority (Single Focus Mode)
-- 1. Updates importer_prioritize_work with conflict detection and force replace
-- 2. Updates importer_acquire_job to enforce strict single focus mode:
--    when a work is in absolute priority, daemon acquires ONLY that work's jobs
--    until all chapters are finished, then auto-completes and resumes regular queue.
-- 3. Updates importer_cancel_staff_request to permit canceling active IMPORTING runs.
-- ==============================================================================

-- 1. RPC TO PRIORITIZE WORK WITH CONFLICT DETECTION & REPLACE
create or replace function public.importer_prioritize_work(
  p_work_id uuid,
  p_reason text default null,
  p_force_replace boolean default false
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_active_request_id uuid;
  v_active_work_id uuid;
  v_active_title text;
  v_request_id uuid;
  v_source text;
  v_source_work_id text;
begin
  if not public.is_editor() then
    raise exception 'Apenas membros da equipe editorial podem priorizar obras.';
  end if;

  -- Detect existing active focus request
  select sr.id, sr.work_id into v_active_request_id, v_active_work_id
  from public.importer_staff_requests sr
  where sr.status in ('QUEUED', 'IMPORTING')
  order by sr.created_at desc
  limit 1;

  if v_active_work_id is not null then
    if v_active_work_id = p_work_id then
      return jsonb_build_object(
        'success', true,
        'already_queued', true,
        'message', 'Esta obra já está em Prioridade Absoluta na fila.'
      );
    end if;

    if not p_force_replace then
      select title into v_active_title
      from public.works
      where id = v_active_work_id;

      return jsonb_build_object(
        'success', false,
        'conflict', true,
        'active_work_id', v_active_work_id,
        'active_work_title', coalesce(v_active_title, 'Obra anterior'),
        'message', 'Já existe uma obra em Prioridade Absoluta (' || coalesce(v_active_title, 'Outra obra') || ').'
      );
    else
      -- Force replace: cancel previously active requests
      update public.importer_staff_requests
      set status = 'CANCELLED', updated_at = now()
      where status in ('QUEUED', 'IMPORTING');
    end if;
  end if;

  -- Create new absolute priority request
  insert into public.importer_staff_requests (
    work_id,
    requested_by,
    priority_boost,
    reason,
    status
  ) values (
    p_work_id,
    auth.uid(),
    100,
    p_reason,
    'QUEUED'
  )
  returning id into v_request_id;

  -- Boost priority of any existing QUEUED jobs for this work to 100
  update public.importer_queue
  set priority = 100, updated_at = now()
  where (payload->>'workId')::text = p_work_id::text
    and status = 'QUEUED';

  -- Lookup mapped source and source_work_id
  select source, source_work_id into v_source, v_source_work_id
  from public.importer_work_mappings
  where work_id = p_work_id
  order by confidence desc nulls last
  limit 1;

  if v_source is null then
    v_source := 'nexus';
  end if;

  -- If no pending or running jobs exist for this work, enqueue SYNC_WORK immediately
  if not exists (
    select 1 from public.importer_queue
    where (payload->>'workId')::text = p_work_id::text
      and status in ('QUEUED', 'IMPORTING', 'RETRY')
  ) then
    insert into public.importer_queue (
      task_type,
      source,
      dedupe_key,
      payload,
      priority,
      status
    ) values (
      'SYNC_WORK',
      v_source,
      'staff:sync:' || p_work_id::text || ':' || extract(epoch from now())::bigint,
      jsonb_build_object(
        'workId', p_work_id,
        'sourceWorkId', v_source_work_id,
        'staffRequested', true,
        'requestId', v_request_id
      ),
      100,
      'QUEUED'
    )
    on conflict (dedupe_key) do nothing;
  end if;

  return jsonb_build_object(
    'success', true,
    'request_id', v_request_id,
    'message', 'Obra colocada em Prioridade Absoluta com sucesso!'
  );
end;
$$;

revoke all on function public.importer_prioritize_work(uuid, text, boolean) from public, anon;
grant execute on function public.importer_prioritize_work(uuid, text, boolean) to authenticated;

-- 2. RPC TO CANCEL STAFF REQUEST (ALLOW CANCEL IN QUEUED OR IMPORTING)
create or replace function public.importer_cancel_staff_request(
  p_request_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_updated integer;
begin
  if not public.is_editor() then
    raise exception 'Apenas membros da equipe editorial podem cancelar solicitações.';
  end if;

  update public.importer_staff_requests
  set status = 'CANCELLED', updated_at = now()
  where id = p_request_id
    and status in ('QUEUED', 'IMPORTING');

  get diagnostics v_updated = row_count;

  if v_updated > 0 then
    return jsonb_build_object('success', true, 'message', 'Prioridade cancelada com sucesso.');
  else
    return jsonb_build_object('success', false, 'message', 'Solicitação não pôde ser cancelada.');
  end if;
end;
$$;

revoke all on function public.importer_cancel_staff_request(uuid) from public, anon;
grant execute on function public.importer_cancel_staff_request(uuid) to authenticated;

-- 3. UPDATE IMPORTER_ACQUIRE_JOB WITH SINGLE-FOCUS ABSOLUTE PRIORITY
create or replace function public.importer_acquire_job(
  p_worker_id text,
  p_lease_duration interval default interval '5 minutes',
  p_source text default null
)
returns table (
  id uuid,
  task_type text,
  source text,
  priority integer,
  payload jsonb,
  dedupe_key text,
  status text,
  attempts integer,
  max_attempts integer,
  locked_by text,
  locked_at timestamptz,
  lease_expires_at timestamptz,
  next_run_at timestamptz,
  last_error text,
  chapter_sort_key numeric
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_focus_request_id uuid;
  v_focus_work_id uuid;
  v_focus_created_at timestamptz;
  v_has_pending_jobs boolean;
  v_has_pending_mappings boolean;
  v_job_id uuid;
begin
  -- 1. Check if there is an active focus work (Absolute Priority)
  select sr.id, sr.work_id, sr.created_at into v_focus_request_id, v_focus_work_id, v_focus_created_at
  from public.importer_staff_requests sr
  where sr.status in ('QUEUED', 'IMPORTING')
  order by sr.created_at desc
  limit 1;

  if v_focus_work_id is not null then
    -- Check if focus work still has active queue tasks
    select exists (
      select 1 from public.importer_queue q_focus
      where (q_focus.payload->>'workId')::text = v_focus_work_id::text
        and q_focus.status in ('QUEUED', 'IMPORTING', 'RETRY')
    ) into v_has_pending_jobs;

    -- Check if focus work still has pending, importing, or staged mappings
    select exists (
      select 1 from public.importer_chapter_mappings cm
      where cm.work_id = v_focus_work_id
        and cm.status in ('PENDING', 'IMPORTING', 'STAGED')
    ) into v_has_pending_mappings;

    -- If no pending jobs and no pending mappings remain, auto-complete focus mode
    -- Only auto-completes if request is older than 30 seconds to allow sync/discovery to enqueue chapters
    if not v_has_pending_jobs and not v_has_pending_mappings and (v_focus_created_at < now() - interval '30 seconds') then
      update public.importer_staff_requests
      set status = 'COMPLETED', updated_at = now()
      where public.importer_staff_requests.id = v_focus_request_id;

      v_focus_work_id := null;
      v_focus_request_id := null;
    else
      -- Advance status from QUEUED to IMPORTING
      update public.importer_staff_requests
      set status = 'IMPORTING', updated_at = now()
      where public.importer_staff_requests.id = v_focus_request_id
        and public.importer_staff_requests.status = 'QUEUED';
    end if;
  end if;

  -- 2. Select next job respecting focus mode
  select q.id into v_job_id
  from public.importer_queue q
  where (
    (q.status in ('QUEUED', 'RETRY') and q.next_run_at <= now())
    or
    (q.status = 'IMPORTING' and q.lease_expires_at < now())
  )
  -- Single-focus constraint: when active, acquire ONLY jobs belonging to focus work
  and (
    v_focus_work_id is null
    or
    (q.payload->>'workId')::text = v_focus_work_id::text
  )
  and (p_source is null or q.source = p_source)
  and not exists (
    select 1 from public.importer_sources s
    where s.id = q.source
      and (s.enabled = false or s.status = 'PAUSED' or (s.cooldown_until is not null and s.cooldown_until > now()))
  )
  order by
    case
      -- 0. Absolute Priority Work jobs always jump to the top
      when v_focus_work_id is not null and (q.payload->>'workId')::text = v_focus_work_id::text then 100

      -- 1. Progressive Blocker (Canonical Barrier)
      when q.task_type = 'IMPORT_CHAPTER'
       and q.payload->>'workId' is not null
       and q.chapter_sort_key is not null
       and exists (
         select 1
         from public.importer_chapter_mappings staged
         where staged.work_id = (q.payload->>'workId')::uuid
           and staged.status = 'STAGED'
           and staged.chapter_sort_key > q.chapter_sort_key
       )
       and not exists (
         select 1
         from public.importer_queue q2
         where q2.task_type = 'IMPORT_CHAPTER'
           and (q2.payload->>'workId')::text = q.payload->>'workId'
           and q2.status in ('QUEUED', 'RETRY', 'IMPORTING')
           and q2.id != q.id
           and q2.chapter_sort_key < q.chapter_sort_key
       )
       and not exists (
         select 1
         from public.importer_chapter_mappings m2
         left join public.chapters c2 on c2.id = m2.chapter_id
         where m2.work_id = (q.payload->>'workId')::uuid
           and m2.chapter_sort_key < q.chapter_sort_key
           and m2.status not in ('STAGED', 'COMPLETED')
           and m2.is_gap = false
           and (c2.published_at is null or c2.id is null)
       ) then 90

      else q.priority
    end desc,
    q.chapter_sort_key asc nulls last,
    q.next_run_at asc,
    q.created_at asc
  limit 1
  for update skip locked;

  if v_job_id is not null then
    return query
    update public.importer_queue
    set
      status = 'IMPORTING',
      locked_by = p_worker_id,
      locked_at = now(),
      lease_expires_at = now() + p_lease_duration,
      attempts = public.importer_queue.attempts + 1,
      updated_at = now()
    where public.importer_queue.id = v_job_id
    returning
      public.importer_queue.id,
      public.importer_queue.task_type,
      public.importer_queue.source,
      public.importer_queue.priority,
      public.importer_queue.payload,
      public.importer_queue.dedupe_key,
      public.importer_queue.status,
      public.importer_queue.attempts,
      public.importer_queue.max_attempts,
      public.importer_queue.locked_by,
      public.importer_queue.locked_at,
      public.importer_queue.lease_expires_at,
      public.importer_queue.next_run_at,
      public.importer_queue.last_error,
      public.importer_queue.chapter_sort_key;
  end if;
end;
$$;

revoke all on function public.importer_acquire_job(text, interval, text) from public, anon, authenticated;
grant execute on function public.importer_acquire_job(text, interval, text) to service_role;
