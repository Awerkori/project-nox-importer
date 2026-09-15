import puppeteer from 'puppeteer-core';
import fs from 'fs';

async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  let supabasePage = pages.find(p => p.url().includes('supabase.com'));
  
  const token = await supabasePage.evaluate(() => window.localStorage.getItem('supabase.dashboard.auth.token'));
  if (!token) process.exit(1);
  const jwt = JSON.parse(token).access_token;
  
  const sql = `
  WITH cancelled_jobs AS (
    SELECT id, (payload->>'workId')::uuid as work_id, coalesce(chapter_sort_key, (payload->>'chapterNumber')::numeric) as sort_key
    FROM public.importer_queue
    WHERE status = 'CANCELLED_BY_STAFF' AND task_type = 'IMPORT_CHAPTER'
  )
  SELECT c.work_id, c.sort_key, count(m.id) as blocked_staged_count
  FROM cancelled_jobs c
  JOIN public.importer_chapter_mappings m 
    ON m.work_id = c.work_id AND m.chapter_sort_key > c.sort_key AND m.status = 'STAGED'
  GROUP BY c.work_id, c.sort_key
  ORDER BY blocked_staged_count DESC;
  `;
  
  const res = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
    body: JSON.stringify({ query: sql })
  });
  
  console.log(await res.text());
  await browser.disconnect();
}
run().catch(console.error);
