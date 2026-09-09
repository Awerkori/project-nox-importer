-- ==============================================================================
-- Project Nox Manga - Migration 20260909040000:
-- Persistent Jobs & Absolute Staff Priority
-- 1. Expands importer_staff_requests schema with human cancellation tracking
--    (cancelled_by, cancelled_at, cancel_reason) and retry telemetry
--    (last_error, last_attempt_at, next_attempt_at, attempt_count).
-- 2. Disallows automatic cancellation of Absolute Priorities on technical errors.
-- 3. Updates importer_acquire_job to support RETRYING and pause all non-priority
--    acquisitions during retry backoff periods.
-- 4. Updates importer_recover_stalled_leases to NEVER fail jobs on lease expiration,
--    always requeueing with backoff.
-- 5. Updates importer_cancel_staff_request and importer_prioritize_work with
--    explicit audit tracking.
-- ==============================================================================

begin;

-- 1. UPDATE IMPORTER_STAFF_REQUESTS SCHEMA
alter table public.importer_staff_requests
  drop constraint if exists importer_staff_requests_status_check;

alter table public.importer_staff_requests
  add constraint importer_staff_requests_status_check
  check (status in ('QUEUED', 'IMPORTING', 'RETRYING', 'COMPLETED', 'CANCELLED'));

alter table public.importer_staff_requests
  add column if not exists cancelled_by uuid references public.members(id) on delete set null,
  add column if not exists cancelled_at timestamptz,
  add column if not exists cancel_reason text,
  add column if not exists last_error text,
  add column if not exists last_attempt_at timestamptz,
  add column if not exists next_attempt_at timestamptz,
  add column if not exists attempt_count integer not null default 0;

grant select (cancelled_by, cancelled_at, cancel_reason, last_error, last_attempt_at, next_attempt_at, attempt_count)
  on public.importer_staff_requests to authenticated;
grant update (cancelled_by, cancelled_at, cancel_reason, last_error, last_attempt_at, next_attempt_at, attempt_count)
  on public.importer_staff_requests to authenticated;

-- 2. RPC TO CANCEL STAFF REQUEST (HUMAN ONLY)
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
  v_work_id uuid;
begin
  if not public.is_editor() then
    raise exception 'Apenas membros da equipe editorial podem cancelar solicitações.';
  end if;

  select work_id into v_work_id
  from public.importer_staff_requests
  where id = p_request_id;

  update public.importer_staff_requests
  set
    status = 'CANCELLED',
    cancelled_by = auth.uid(),
    cancelled_at = now(),
    cancel_reason = 'STAFF_CANCELLED',
    updated_at = now()
  where id = p_request_id
    and status in ('QUEUED', 'IMPORTING', 'RETRYING');

  get diagnostics v_updated = row_count;

  if v_updated > 0 then
    -- Demote priority of queued/retrying jobs for this work back to standard (30)
    if v_work_id is not null then
      update public.importer_queue
      set priority = 30, updated_at = now()
      where (payload->>'workId')::text = v_work_id::text
        and status in ('QUEUED', 'RETRY')
        and priority > 30;
    end if;

    return jsonb_build_object('success', true, 'message', 'Prioridade cancelada pela Staff com sucesso.');
  else
    return jsonb_build_object('success', false, 'message', 'Solicitação não pôde ser cancelada ou já estava finalizada.');
  end if;
end;
$$;

revoke all on function public.importer_cancel_staff_request(uuid) from public, anon;
grant execute on function public.importer_cancel_staff_request(uuid) to authenticated;

-- 3. RPC TO PRIORITIZE WORK WITH STRICT SINGLE-FOCUS & REPLACEMENT AUDIT
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
  where sr.status in ('QUEUED', 'IMPORTING', 'RETRYING')
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
      -- Force replace: explicitly cancel previously active requests with audit tracking
      update public.importer_staff_requests
      set
        status = 'CANCELLED',
        cancelled_by = auth.uid(),
        cancelled_at = now(),
        cancel_reason = 'REPLACED_BY_STAFF',
        updated_at = now()
      where status in ('QUEUED', 'IMPORTING', 'RETRYING');

      -- Demote priority of previous work jobs to standard
      update public.importer_queue
      set priority = 30, updated_at = now()
      where (payload->>'workId')::text = v_active_work_id::text
        and status in ('QUEUED', 'RETRY')
        and priority > 30;
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

  -- Boost priority of any existing QUEUED / RETRY jobs for this work to 100
  update public.importer_queue
  set priority = 100, updated_at = now()
  where (payload->>'workId')::text = p_work_id::text
    and status in ('QUEUED', 'RETRY');

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

-- 4. UPDATE IMPORTER_ACQUIRE_JOB WITH RETRYING SUPPORT & STRICT FOCUS BLOCKING
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
  v_focus_status text;
  v_has_pending_jobs boolean;
  v_has_pending_mappings boolean;
  v_has_completed_work boolean;
  v_job_id uuid;
begin
  -- 1. Check if there is an active focus work (Absolute Priority)
  select sr.id, sr.work_id, sr.created_at, sr.status
  into v_focus_request_id, v_focus_work_id, v_focus_created_at, v_focus_status
  from public.importer_staff_requests sr
  where sr.status in ('QUEUED', 'IMPORTING', 'RETRYING')
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

    -- Check if at least some work was actually done/completed
    select (
      exists (
        select 1 from public.importer_chapter_mappings cm_done
        where cm_done.work_id = v_focus_work_id and cm_done.status = 'COMPLETED'
      )
      or exists (
        select 1 from public.importer_work_mappings wm_done
        where wm_done.work_id = v_focus_work_id and wm_done.sync_status = 'COMPLETED'
      )
    ) into v_has_completed_work;

    -- Only auto-completes if work actually finished AND request is older than 30 seconds
    if not v_has_pending_jobs and not v_has_pending_mappings and v_has_completed_work and (v_focus_created_at < now() - interval '30 seconds') then
      update public.importer_staff_requests
      set status = 'COMPLETED', updated_at = now()
      where public.importer_staff_requests.id = v_focus_request_id;

      v_focus_work_id := null;
      v_focus_request_id := null;
    elsif v_focus_status = 'QUEUED' then
      -- Transition QUEUED to IMPORTING once processing begins
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
  -- Single-focus constraint: when active, acquire ONLY jobs belonging to focus work.
  -- If all jobs for focus work are in backoff retry, return nothing (strictly pauses other works).
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

-- 5. UPDATE IMPORTER_RECOVER_STALLED_LEASES (NEVER TERMINATE JOBS AS FAILED)
create or replace function public.importer_recover_stalled_leases()
returns table (
  recovered_count integer,
  failed_count integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_recovered integer := 0;
  v_failed integer := 0;
begin
  -- Jobs whose lease expired (crash / timeout / restart) are ALWAYS requeued to RETRY with backoff.
  -- No job ever dies permanently to status = 'FAILED' from lease expiration.
  with requeued_jobs as (
    update public.importer_queue
    set
      status = 'RETRY',
      locked_by = null,
      locked_at = null,
      lease_expires_at = null,
      next_run_at = now(),
      last_error = coalesce(last_error || ' | ', '') || 'Lease expirado (tentativa ' || attempts || ')',
      updated_at = now()
    where status = 'IMPORTING'
      and lease_expires_at < now()
    returning id
  )
  select count(*)::integer into v_recovered from requeued_jobs;

  return query select v_recovered, v_failed;
end;
$$;

revoke all on function public.importer_recover_stalled_leases() from public, anon, authenticated;
grant execute on function public.importer_recover_stalled_leases() to service_role;

commit;
