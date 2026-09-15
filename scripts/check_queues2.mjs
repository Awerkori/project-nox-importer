import puppeteer from 'puppeteer-core';
async function run() {
  const res = await fetch('http://127.0.0.1:9222/json/version');
  const { webSocketDebuggerUrl } = await res.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 5000));
  
  const result = await page.evaluate(async () => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    const jwt = JSON.parse(token).access_token;
    
    // Check queue stats
    const queueQuery = `
      SELECT status, COUNT(*) 
      FROM importer_queue 
      GROUP BY status 
      ORDER BY status;
    `;
    const queueData = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: queueQuery })
    }).then(r => r.json());
    
    const chaptersQuery = `
      SELECT 
        CASE WHEN published_at IS NULL THEN 'STAGED' ELSE 'PUBLISHED' END as state, 
        COUNT(*) 
      FROM chapters 
      GROUP BY 1 
      ORDER BY 1;
    `;
    const chaptersData = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: chaptersQuery })
    }).then(r => r.json());
    
    // Check active jobs count
    const activeJobsQuery = `
      SELECT COUNT(*) as active_jobs
      FROM importer_queue
      WHERE status = 'IMPORTING' AND updated_at > NOW() - INTERVAL '30 minutes';
    `;
    const activeJobsData = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: activeJobsQuery })
    }).then(r => r.json());
    
    return {
      queueData,
      chaptersData,
      activeJobsData
    };
  });
  
  console.log(JSON.stringify(result, null, 2));
  await browser.disconnect();
}
run().catch(console.error);
