import puppeteer from 'puppeteer-core';

// Fix mappings stuck as PENDING when their corresponding job is COMPLETED/FAILED
// These are "orphaned" mappings — the job finished but the mapping didn't update
// Safe: only touches PENDING mappings with a COMPLETED/FAILED job and no QUEUED/RETRY/IMPORTING
const sql = `
-- Count orphaned PENDING mappings
SELECT
  m.work_id,
  m.chapter_sort_key,
  m.source,
  m.status AS mapping_status,
  q.status AS job_status,
  q.last_error
FROM public.importer_chapter_mappings m
JOIN public.importer_queue q ON (
  (q.payload->>'workId')::text = m.work_id::text
  AND q.chapter_sort_key = m.chapter_sort_key
  AND q.source = m.source
  AND q.status = 'COMPLETED'
)
WHERE m.status = 'PENDING'
  AND NOT EXISTS (
    SELECT 1 FROM public.importer_queue q2
    WHERE (q2.payload->>'workId')::text = m.work_id::text
      AND q2.chapter_sort_key = m.chapter_sort_key
      AND q2.source = m.source
      AND q2.status IN ('QUEUED', 'RETRY', 'IMPORTING')
  )
LIMIT 20;
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
  console.log("Orphan query:", result.status, result.body.slice(0, 1000));
  await browser.disconnect();
}
run().catch(console.error);
