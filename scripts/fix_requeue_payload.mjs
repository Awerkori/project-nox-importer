import puppeteer from 'puppeteer-core';
// Fix the payload of the requeued jobs
const sql = `
UPDATE public.importer_queue q
SET payload = jsonb_build_object(
  'workId', m.work_id::text,
  'chapterSortKey', m.chapter_sort_key,
  'chapterNumber', m.chapter_number,
  'sourceChapterId', m.source_chapter_id,
  'workMappingId', m.work_mapping_id,
  'requeued', true
)
FROM public.importer_chapter_mappings m
WHERE q.dedupe_key LIKE 'requeue:%'
  AND (q.payload->>'workId')::text = m.work_id::text
  AND q.chapter_sort_key = m.chapter_sort_key
  AND q.source = m.source;
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
