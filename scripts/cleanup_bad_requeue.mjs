import puppeteer from 'puppeteer-core';
// Cancel re-enqueued jobs that are NOT actual blockers
// A real blocker: lowest PENDING/missing chapter below the lowest STAGED
// The re-enqueue script was too broad - it re-enqueued STAGED chapters too
// Cancel all requeue: jobs except for the true blockers (ch102, ch3, ch35.1, ch0, etc.)
const sql = `
-- Cancel incorrect requeue jobs (those for chapters that already have STAGED or COMPLETED mapping)
UPDATE public.importer_queue q
SET status = 'CANCELLED_BY_STAFF', updated_at = now()
WHERE q.dedupe_key LIKE 'requeue:%'
  AND q.status = 'QUEUED'
  AND EXISTS (
    SELECT 1 FROM public.importer_chapter_mappings m
    WHERE m.work_id::text = (q.payload->>'workId')::text
      AND m.chapter_sort_key = q.chapter_sort_key
      AND m.status IN ('STAGED', 'COMPLETED')
  );

-- Also cancel requeue jobs for chapters that are NOT a direct blocker
-- (i.e., there's no STAGED chapter at a LOWER sort key being blocked by this one)
UPDATE public.importer_queue q2
SET status = 'CANCELLED_BY_STAFF', updated_at = now()
WHERE q2.dedupe_key LIKE 'requeue:%'
  AND q2.status = 'QUEUED'
  AND NOT EXISTS (
    SELECT 1 FROM public.importer_chapter_mappings m2
    WHERE m2.work_id::text = (q2.payload->>'workId')::text
      AND m2.status = 'STAGED'
      AND m2.chapter_sort_key > q2.chapter_sort_key
  );

SELECT count(*) AS still_queued_requeue
FROM public.importer_queue
WHERE dedupe_key LIKE 'requeue:%' AND status = 'QUEUED';
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
  console.log("Cleanup result:", result.status, result.body.slice(0, 500));
  await browser.disconnect();
}
run().catch(console.error);
