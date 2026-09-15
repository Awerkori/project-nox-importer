import puppeteer from 'puppeteer-core';
async function run() {
  const res = await fetch('http://127.0.0.1:9222/json/version');
  const { webSocketDebuggerUrl } = await res.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 8000));
  
  const sql = `
  CREATE INDEX IF NOT EXISTS idx_importer_queue_fairness 
  ON public.importer_queue (status, task_type, next_run_at) 
  INCLUDE (priority, chapter_sort_key, work_partition_key, source);
  `;
  
  const result = await page.evaluate(async (query) => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    const jwt = JSON.parse(token).access_token;
    const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query })
    });
    return await response.text();
  }, sql);
  
  console.log("Index created:", result);
  await browser.disconnect();
}
run().catch(console.error);
