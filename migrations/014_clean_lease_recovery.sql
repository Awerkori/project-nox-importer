-- ==============================================================================
-- Migration: 014_clean_lease_recovery.sql
-- Description: Clean recovered lease errors, update importer_recover_stalled_leases,
--              importer_acquire_job, and importer_release_job so that recovered leases
--              never remain as active error cards.
-- ==============================================================================

-- 1. UPDATE IMPORTER_RECOVER_STALLED_LEASES
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
  -- Recovered leases must NOT remain as active error cards.
  -- last_error is set to null, last_recovered_error is recorded, and status is set to QUEUED.
  with requeued_jobs as (
    update public.importer_queue
    set
      status = 'QUEUED',
      locked_by = null,
      locked_at = null,
      lease_expires_at = null,
      next_run_at = now(),
      last_recovered_error = coalesce(last_error, 'Lease expirado (recuperado automaticamente na tentativa ' || attempts || ')'),
      recovered_at = now(),
      retry_reason = 'LEASE_EXPIRED_RECOVERED',
      last_error = null,
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

-- 2. UPDATE IMPORTER_ACQUIRE_JOB TO RESET last_error ON ACQUISITION
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

  if v_focus_request_id is not null then
    -- Check if any work remains for the focus request
    select exists (
      select 1 from public.importer_queue q
      where (q.payload->>'workId')::text = v_focus_work_id::text
        and q.status in ('QUEUED', 'RETRY', 'IMPORTING')
    ) into v_has_pending_jobs;

    select exists (
      select 1 from public.importer_chapter_mappings m
      where m.work_id = v_focus_work_id
        and m.status in ('PENDING', 'IMPORTING', 'STAGED')
    ) into v_has_pending_mappings;

    select (c.published_at is not null) into v_has_completed_work
    from public.chapters c
    where c.work_id = v_focus_work_id
    order by c.created_at desc
    limit 1;

    -- If the work has finished importing all items, mark focus request COMPLETED
    if not v_has_pending_jobs and not v_has_pending_mappings and coalesce(v_has_completed_work, false) then
      update public.importer_staff_requests
      set status = 'COMPLETED', updated_at = now()
      where id = v_focus_request_id;

      v_focus_request_id := null;
      v_focus_work_id := null;
    elsif v_focus_status = 'QUEUED' then
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
  and (
    v_focus_work_id is null
    or
    (q.payload->>'workId')::text = v_focus_work_id::text
  )
  and (p_source is null or q.source = p_source)
  and not exists (
    select 1 from public.importer_sources s
    where s.id = q.source
      and (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now()))
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
      last_error = null,
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

-- 3. UPDATE IMPORTER_RENEW_LEASE
create or replace function public.importer_renew_lease(
  p_job_id uuid,
  p_worker_id text,
  p_lease_duration interval default interval '5 minutes'
)
returns boolean
language plpgsql
security definer
set search_path = public
as $$
begin
  update public.importer_queue
  set
    lease_expires_at = now() + p_lease_duration,
    updated_at = now()
  where id = p_job_id
    and (locked_by = p_worker_id or locked_by is null)
    and status = 'IMPORTING';

  return found;
end;
$$;

revoke all on function public.importer_renew_lease(uuid, text, interval) from public, anon, authenticated;
grant execute on function public.importer_renew_lease(uuid, text, interval) to service_role;

-- 4. UPDATE IMPORTER_RELEASE_JOB TO CLEAR last_error ON COMPLETED
create or replace function public.importer_release_job(
  p_job_id uuid,
  p_worker_id text,
  p_status text,
  p_error text default null,
  p_retry_delay interval default null
)
returns boolean
language plpgsql
security definer
set search_path = public
as $$
declare
  v_next_run timestamptz;
  v_final_error text;
begin
  if p_status = 'RETRY' then
    v_next_run := now() + coalesce(p_retry_delay, interval '1 minute');
    v_final_error := p_error;
  elsif p_status = 'COMPLETED' then
    v_next_run := now();
    v_final_error := null;
  else
    v_next_run := now();
    v_final_error := p_error;
  end if;

  update public.importer_queue
  set
    status = p_status,
    locked_by = null,
    locked_at = null,
    lease_expires_at = null,
    last_error = v_final_error,
    next_run_at = v_next_run,
    updated_at = now()
  where id = p_job_id
    and (locked_by = p_worker_id or locked_by is null);

  return found;
end;
$$;

revoke all on function public.importer_release_job(uuid, text, text, text, interval) from public, anon, authenticated;
grant execute on function public.importer_release_job(uuid, text, text, text, interval) to service_role;
