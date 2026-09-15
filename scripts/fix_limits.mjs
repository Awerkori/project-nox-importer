import puppeteer from 'puppeteer-core';
const sql = `
DROP FUNCTION IF EXISTS importer_acquire_job(text,interval,text,text);
CREATE OR REPLACE FUNCTION importer_acquire_job(p_worker_id text, p_lease_duration interval, p_source text DEFAULT NULL::text, p_task_type text DEFAULT NULL::text) RETURNS TABLE(id uuid, task_type text, source text, priority integer, payload jsonb, dedupe_key text, status text, attempts integer, max_attempts integer, locked_by text, locked_at timestamp with time zone, lease_expires_at timestamp with time zone, next_run_at timestamp with time zone, last_error text, chapter_sort_key numeric)
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_focus_request_id uuid;
  v_focus_work_id uuid;
  v_focus_created_at timestamptz;
  v_focus_status text;
  v_has_pending_jobs boolean;
  v_has_pending_mappings boolean;
  v_has_completed_work boolean;
  v_job_id uuid;
  v_barrier_state text;
BEGIN
  SELECT value INTO v_barrier_state FROM public.settings WHERE key = 'publication_safety_barrier';

  SELECT sr.id, sr.work_id, sr.created_at, sr.status
  INTO v_focus_request_id, v_focus_work_id, v_focus_created_at, v_focus_status
  FROM public.importer_staff_requests sr
  WHERE sr.status in ('QUEUED', 'IMPORTING', 'RETRYING')
  ORDER BY sr.created_at desc LIMIT 1;

  IF v_focus_request_id is not null THEN
    SELECT exists (SELECT 1 FROM public.importer_queue q WHERE (q.payload->>'workId')::text = v_focus_work_id::text AND q.status in ('QUEUED', 'RETRY', 'IMPORTING')) INTO v_has_pending_jobs;
    SELECT exists (SELECT 1 FROM public.importer_chapter_mappings m WHERE m.work_id = v_focus_work_id AND m.status in ('PENDING', 'IMPORTING', 'STAGED')) INTO v_has_pending_mappings;
    SELECT (c.published_at is not null) INTO v_has_completed_work FROM public.chapters c WHERE c.work_id = v_focus_work_id ORDER BY c.created_at desc LIMIT 1;

    IF not v_has_pending_jobs and not v_has_pending_mappings and coalesce(v_has_completed_work, false) THEN
      UPDATE public.importer_staff_requests SET status = 'COMPLETED', updated_at = now() WHERE public.importer_staff_requests.id = v_focus_request_id;
      v_focus_request_id := null;
      v_focus_work_id := null;
    ELSIF v_focus_status = 'QUEUED' THEN
      UPDATE public.importer_staff_requests SET status = 'IMPORTING', updated_at = now() WHERE public.importer_staff_requests.id = v_focus_request_id AND public.importer_staff_requests.status = 'QUEUED';
    END IF;
  END IF;

  
  SELECT q.id INTO v_job_id
  FROM (
    SELECT cand_batch.id, cand_batch.payload, cand_batch.task_type, cand_batch.chapter_sort_key, cand_batch.priority, cand_batch.created_at
    FROM (
      ( SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority >= 100 AND q_cand.next_run_at <= now()
        AND (v_focus_work_id is null or (q_cand.payload->>'workId')::text = v_focus_work_id::text)
        AND (p_source is null or q_cand.source = p_source)
        AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q_cand.task_type = p_task_type)
        AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        AND not exists (SELECT 1 FROM public.importer_sources s WHERE s.id = q_cand.source AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now())))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 200 )
      UNION ALL
      ( SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority >= 80 AND q_cand.priority < 100 AND q_cand.next_run_at <= now()
        AND (v_focus_work_id is null or (q_cand.payload->>'workId')::text = v_focus_work_id::text)
        AND (p_source is null or q_cand.source = p_source)
        AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q_cand.task_type = p_task_type)
        AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        AND not exists (SELECT 1 FROM public.importer_sources s WHERE s.id = q_cand.source AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now())))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 200 )
      UNION ALL
      ( SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority < 80 AND q_cand.next_run_at <= now()
        AND (v_focus_work_id is null or (q_cand.payload->>'workId')::text = v_focus_work_id::text)
        AND (p_source is null or q_cand.source = p_source)
        AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q_cand.task_type = p_task_type)
        AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        AND not exists (SELECT 1 FROM public.importer_sources s WHERE s.id = q_cand.source AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now())))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 100 )
      UNION ALL
      ( SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.next_run_at <= now()
        AND q_cand.task_type = 'IMPORT_CHAPTER'
        AND q_cand.chapter_sort_key IS NOT NULL
        AND exists (
          SELECT 1 FROM public.importer_chapter_mappings staged
          WHERE staged.work_id = (q_cand.payload->>'workId')::uuid
            AND staged.status = 'STAGED'
            AND staged.chapter_sort_key > q_cand.chapter_sort_key
        )
        AND not exists (SELECT 1 FROM public.importer_sources s WHERE s.id = q_cand.source AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now())))
      ORDER BY q_cand.chapter_sort_key ASC LIMIT 200 )
    ) cand_batch
    ORDER BY
      CASE WHEN v_focus_work_id is not null and (cand_batch.payload->>'workId')::text = v_focus_work_id::text THEN 100000 ELSE 0 END DESC,
      CASE WHEN p_task_type = 'DISCOVERY' AND cand_batch.task_type = 'DISCOVER_WORKS' THEN 10000 ELSE 0 END DESC,
      CASE WHEN p_task_type = 'DISCOVERY' AND cand_batch.task_type = 'SYNC_WORK' THEN 9000 ELSE 0 END DESC,
      (
        cand_batch.priority 
        + CASE WHEN cand_batch.task_type = 'IMPORT_CHAPTER' 
                AND cand_batch.payload->>'workId' is not null 
                AND cand_batch.chapter_sort_key is not null 
                AND exists (
                  SELECT 1 FROM public.importer_chapter_mappings staged
                  WHERE staged.work_id = (cand_batch.payload->>'workId')::uuid
                    AND staged.status = 'STAGED'
                    AND staged.chapter_sort_key > cand_batch.chapter_sort_key
                ) THEN 5000 ELSE 0 END
        - CASE WHEN cand_batch.payload->>'workId' is not null THEN (
            SELECT count(*) * 1000
            FROM public.importer_queue active_q
            WHERE active_q.status = 'IMPORTING' 
              AND active_q.task_type = 'IMPORT_CHAPTER'
              AND (active_q.payload->>'workId')::text = (cand_batch.payload->>'workId')::text
          ) ELSE 0 END
      ) DESC,
      CASE WHEN cand_batch.chapter_sort_key IS NOT NULL THEN cand_batch.chapter_sort_key ELSE 999999 END ASC,
      cand_batch.created_at ASC
  ) sorted_cands
  JOIN public.importer_queue q ON q.id = sorted_cands.id
  FOR UPDATE OF q SKIP LOCKED
  LIMIT 1;

  IF v_job_id IS NULL THEN
    SELECT q.id INTO v_job_id
    FROM public.importer_queue q
    WHERE q.id = (
      SELECT q_cand.id FROM public.importer_queue q_cand
      WHERE q_cand.status = 'IMPORTING' AND q_cand.lease_expires_at < now()
        AND (p_source is null or q_cand.source = p_source)
        AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q_cand.task_type = p_task_type)
      ORDER BY q_cand.lease_expires_at ASC LIMIT 1
    )
    FOR UPDATE OF q SKIP LOCKED;
  END IF;

  IF v_job_id IS NULL THEN RETURN; END IF;

  RETURN QUERY
  UPDATE public.importer_queue SET status = 'IMPORTING', locked_by = p_worker_id, locked_at = now(), lease_expires_at = now() + p_lease_duration, attempts = public.importer_queue.attempts + 1, updated_at = now()
  WHERE public.importer_queue.id = v_job_id
  RETURNING public.importer_queue.id, public.importer_queue.task_type, public.importer_queue.source, public.importer_queue.priority, public.importer_queue.payload, public.importer_queue.dedupe_key, public.importer_queue.status, public.importer_queue.attempts, public.importer_queue.max_attempts, public.importer_queue.locked_by, public.importer_queue.locked_at, public.importer_queue.lease_expires_at, public.importer_queue.next_run_at, public.importer_queue.last_error, public.importer_queue.chapter_sort_key;
END;
$$;
`;

async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 4000));
  const res = await page.evaluate(async (query) => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    const jwt = JSON.parse(token).access_token;
    const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query })
    });
    return { status: response.status, body: await response.text() };
  }, sql);
  console.log(res.status, res.body);
  await page.close();
  await browser.disconnect();
}
run().catch(console.error);
