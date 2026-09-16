import puppeteer from 'puppeteer-core';
import fs from 'fs';

// We fix the ambiguous column by fully qualifying everything and not selecting it naked in the union 
// if it conflicts. Actually, inside the function, we can just qualify it as `public.importer_queue.chapter_sort_key` 
// but the easiest is just `#variable_conflict use_column`. 
// Let's put `#variable_conflict use_column` at the exact correct spot!

const sql = `
DROP FUNCTION IF EXISTS importer_acquire_job(text,interval,text,text);
CREATE OR REPLACE FUNCTION importer_acquire_job(p_worker_id text, p_lease_duration interval, p_source text DEFAULT NULL::text, p_task_type text DEFAULT NULL::text) RETURNS TABLE(id uuid, task_type text, source text, priority integer, payload jsonb, dedupe_key text, status text, attempts integer, max_attempts integer, locked_by text, locked_at timestamp with time zone, lease_expires_at timestamp with time zone, next_run_at timestamp with time zone, last_error text, chapter_sort_key numeric)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
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
      UPDATE public.importer_staff_requests SET status = 'COMPLETED', completed_at = now() WHERE public.importer_staff_requests.id = v_focus_request_id;
      v_focus_work_id := null;
    END IF;
  END IF;

  RETURN QUERY
  WITH staged_works AS (
    SELECT work_id, MAX(m.chapter_sort_key) as max_staged_sort_key
    FROM public.importer_chapter_mappings m
    WHERE m.status = 'STAGED'
    GROUP BY work_id
  )
  UPDATE public.importer_queue q
  SET status = 'IMPORTING',
      locked_by = p_worker_id,
      locked_at = now(),
      lease_expires_at = now() + p_lease_duration,
      attempts = q.attempts + 1
  FROM (
    SELECT sorted_cands.id
    FROM (
      SELECT cand_batch.id, cand_batch.payload, cand_batch.task_type, cand_batch.chapter_sort_key, cand_batch.priority, cand_batch.created_at, cand_batch.source
      FROM (
        ( SELECT q1.id, q1.task_type, q1.priority, q1.payload, q1.chapter_sort_key, q1.created_at, q1.source
        FROM public.importer_queue q1
        WHERE q1.status in ('QUEUED', 'RETRY') AND q1.priority >= 100 AND q1.next_run_at <= now()
          AND (v_focus_work_id is null or (q1.payload->>'workId')::text = v_focus_work_id::text)
          AND (p_source is null or q1.source = p_source)
          AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q1.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q1.task_type = p_task_type)
          AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q1.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        ORDER BY q1.priority DESC, q1.next_run_at ASC LIMIT 100 )
        UNION ALL
        ( SELECT q2.id, q2.task_type, q2.priority, q2.payload, q2.chapter_sort_key, q2.created_at, q2.source
        FROM public.importer_queue q2
        WHERE q2.status in ('QUEUED', 'RETRY') AND q2.priority >= 80 AND q2.priority < 100 AND q2.next_run_at <= now()
          AND (v_focus_work_id is null or (q2.payload->>'workId')::text = v_focus_work_id::text)
          AND (p_source is null or q2.source = p_source)
          AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q2.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q2.task_type = p_task_type)
          AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q2.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        ORDER BY q2.priority DESC, q2.next_run_at ASC LIMIT 100 )
        UNION ALL
        ( SELECT q3.id, q3.task_type, q3.priority, q3.payload, q3.chapter_sort_key, q3.created_at, q3.source
        FROM public.importer_queue q3
        WHERE q3.status in ('QUEUED', 'RETRY') AND q3.priority < 80 AND q3.next_run_at <= now()
          AND (v_focus_work_id is null or (q3.payload->>'workId')::text = v_focus_work_id::text)
          AND (p_source is null or q3.source = p_source)
          AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q3.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q3.task_type = p_task_type)
          AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q3.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        ORDER BY q3.priority DESC, q3.next_run_at ASC LIMIT 100 )
        UNION ALL
        ( SELECT q4.id, q4.task_type, q4.priority + 5000 as priority, q4.payload, q4.chapter_sort_key, q4.created_at, q4.source
        FROM staged_works sw
        JOIN public.importer_queue q4 
          ON (q4.payload->>'workId') = sw.work_id::text
        WHERE q4.status in ('QUEUED', 'RETRY') 
          AND q4.next_run_at <= now()
          AND q4.task_type = 'IMPORT_CHAPTER'
          AND q4.chapter_sort_key < sw.max_staged_sort_key
          AND (v_focus_work_id is null or (q4.payload->>'workId')::text = v_focus_work_id::text)
          AND (p_source is null or q4.source = p_source)
        ORDER BY q4.chapter_sort_key ASC LIMIT 100 )
      ) cand_batch
      ORDER BY
        CASE WHEN v_focus_work_id is not null and (cand_batch.payload->>'workId')::text = v_focus_work_id::text THEN 100000 ELSE 0 END DESC,
        CASE WHEN p_task_type = 'DISCOVERY' AND cand_batch.task_type = 'DISCOVER_WORKS' THEN 10000 ELSE 0 END DESC,
        CASE WHEN p_task_type = 'DISCOVERY' AND cand_batch.task_type = 'SYNC_WORK' THEN 9000 ELSE 0 END DESC,
        (
          cand_batch.priority 
          - CASE WHEN cand_batch.payload->>'workId' is not null THEN (
              SELECT count(*) * 1000
              FROM public.importer_queue active_q
              WHERE active_q.status = 'IMPORTING' 
                AND active_q.task_type = 'IMPORT_CHAPTER'
                AND (active_q.payload->>'workId')::text = (cand_batch.payload->>'workId')::text
            ) ELSE 0 END
          - CASE WHEN cand_batch.source is not null THEN (
              SELECT count(*) * 1000
              FROM public.importer_queue active_q
              WHERE active_q.status = 'IMPORTING' 
                AND active_q.source = cand_batch.source
            ) ELSE 0 END
        ) DESC,
        CASE WHEN cand_batch.chapter_sort_key IS NOT NULL THEN cand_batch.chapter_sort_key ELSE 999999 END ASC,
        cand_batch.created_at ASC
    ) sorted_cands
    JOIN public.importer_queue jq ON jq.id = sorted_cands.id
    FOR UPDATE OF jq SKIP LOCKED
    LIMIT 1
  ) to_lock
  WHERE q.id = to_lock.id
  RETURNING q.id, q.task_type, q.source, q.priority, q.payload, q.dedupe_key, q.status, q.attempts, q.max_attempts, q.locked_by, q.locked_at, q.lease_expires_at, q.next_run_at, q.last_error, q.chapter_sort_key;
END;
$$;
`;

async function run() {
  const browser = await puppeteer.launch({
    executablePath: '/usr/bin/chromium',
    userDataDir: '/home/awerkori/.config/chromium',
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' }).catch(e => console.log('Goto timeout ignored'));
  
  await new Promise(r => setTimeout(r, 5000));
  
  const res = await page.evaluate(async (query) => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    const jwt = JSON.parse(token).access_token;
    
    const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query })
    });
    
    await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: "NOTIFY pgrst, 'reload schema';" })
    });

    return { status: response.status, body: await response.text() };
  }, sql);
  
  console.log('Result:', res);
  await browser.close();
}
run().catch(console.error);
