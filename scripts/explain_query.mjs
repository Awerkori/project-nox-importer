import puppeteer from 'puppeteer-core';
async function run() {
  const res = await fetch('http://127.0.0.1:9222/json/version').catch(()=>null);
  if(!res) {
    console.log("No browser running on 9222");
    return;
  }
  const { webSocketDebuggerUrl } = await res.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 2000));
  
  const result = await page.evaluate(async () => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    const jwt = JSON.parse(token).access_token;
    
    const query = `
      EXPLAIN ANALYZE
      WITH staged_works AS (
        SELECT work_id, MAX(chapter_sort_key) as max_staged_sort_key
        FROM public.importer_chapter_mappings
        WHERE status = 'STAGED'
        GROUP BY work_id
      )
      SELECT q_cand.id
      FROM staged_works sw
      JOIN public.importer_queue q_cand 
        ON (q_cand.payload->>'workId') = sw.work_id::text
      WHERE q_cand.status in ('QUEUED', 'RETRY') 
        AND q_cand.next_run_at <= now()
        AND q_cand.task_type = 'IMPORT_CHAPTER'
        AND q_cand.chapter_sort_key < sw.max_staged_sort_key
      ORDER BY q_cand.chapter_sort_key ASC LIMIT 20;
    `;
    const data = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query })
    }).then(r => r.json());
    
    return data;
  });
  
  console.log(result.map(r => r.QUERY_PLAN).join('\n'));
  await browser.disconnect();
}
run().catch(console.error);
