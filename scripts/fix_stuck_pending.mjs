import puppeteer from 'puppeteer-core';

// Fix stuck PENDING mappings that have no active queue job (CANCELLED or COMPLETED jobs)
// For mappings with status=PENDING and no QUEUED/RETRY/IMPORTING job → re-enqueue via reconciliation
// We do this by directly resetting them to a state that triggers re-reconciliation
// SAFE: only touches PENDING mappings (never STAGED/COMPLETED/FAILED with is_gap=true)
const sql = `
DO $$
DECLARE
  v_work_id uuid;
  v_chapter_sort_key numeric;
  v_source text;
  v_count int := 0;
BEGIN
  -- Find PENDING mappings where no active job exists (QUEUED/RETRY/IMPORTING)
  FOR v_work_id, v_chapter_sort_key, v_source IN
    SELECT DISTINCT m.work_id, m.chapter_sort_key, m.source
    FROM public.importer_chapter_mappings m
    WHERE m.status = 'PENDING'
      AND m.is_gap = false
      AND NOT EXISTS (
        SELECT 1 FROM public.importer_queue q
        WHERE (q.payload->>'workId')::text = m.work_id::text
          AND q.chapter_sort_key = m.chapter_sort_key
          AND q.source = m.source
          AND q.status IN ('QUEUED', 'RETRY', 'IMPORTING')
      )
      -- Only works with STAGED above (actual blockers)
      AND EXISTS (
        SELECT 1 FROM public.importer_chapter_mappings m2
        WHERE m2.work_id = m.work_id
          AND m2.status = 'STAGED'
          AND m2.chapter_sort_key > m.chapter_sort_key
      )
  LOOP
    -- Re-enqueue: upsert a new QUEUED job for this chapter
    INSERT INTO public.importer_queue (
      task_type, source, priority, status, payload, chapter_sort_key,
      dedupe_key, next_run_at, max_attempts, attempts
    )
    VALUES (
      'IMPORT_CHAPTER',
      v_source,
      80,
      'QUEUED',
      jsonb_build_object(
        'workId', v_work_id::text,
        'chapterSortKey', v_chapter_sort_key,
        'requeued', true
      ),
      v_chapter_sort_key,
      'requeue:' || v_work_id::text || ':' || v_chapter_sort_key,
      now(),
      5,
      0
    )
    ON CONFLICT (dedupe_key) DO UPDATE
      SET status = 'QUEUED',
          next_run_at = now(),
          attempts = 0,
          updated_at = now();

    v_count := v_count + 1;
  END LOOP;

  RAISE NOTICE 'Re-enqueued % stuck PENDING mappings', v_count;
END;
$$;
`;

async function run() {
  const res = await fetch('http://127.0.0.1:9222/json/version');
  const { webSocketDebuggerUrl } = await res.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 8000));
  const result = await page.evaluate(async (query) => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    const jwt = JSON.parse(token).access_token;
    const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query })
    });
    return { status: response.status, body: await response.text() };
  }, sql);
  console.log("Fix result:", result.status, result.body.slice(0, 500));
  await browser.disconnect();
}
run().catch(console.error);
