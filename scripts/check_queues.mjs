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
      SELECT state, COUNT(*) 
      FROM importer_jobs 
      GROUP BY state 
      ORDER BY state;
    `;
    const queueData = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: queueQuery })
    }).then(r => r.json());
    
    const chaptersQuery = `
      SELECT state, COUNT(*) 
      FROM chapters 
      WHERE state != 'PUBLISHED'
      GROUP BY state 
      ORDER BY state;
    `;
    const chaptersData = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: chaptersQuery })
    }).then(r => r.json());
    
    // Check specific priority works
    const specificQuery = `
      SELECT w.title, w.id, w.priority, COUNT(j.id) as pending_jobs, COUNT(c.id) as blocked_chapters
      FROM works w
      LEFT JOIN importer_jobs j ON j.work_id = w.id AND j.state IN ('QUEUED', 'PENDING', 'ACQUIRED')
      LEFT JOIN chapters c ON c.work_id = w.id AND c.state = 'STAGED'
      WHERE w.title ILIKE '%One Piece%' OR w.title ILIKE '%Batalha%'
      GROUP BY w.id, w.title, w.priority;
    `;
    const specificData = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: specificQuery })
    }).then(r => r.json());
    
    // Check active jobs count
    const activeJobsQuery = `
      SELECT COUNT(*) as active_jobs
      FROM importer_jobs
      WHERE state = 'ACQUIRED' AND locked_at > NOW() - INTERVAL '30 minutes';
    `;
    const activeJobsData = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: activeJobsQuery })
    }).then(r => r.json());
    
    return {
      queueData,
      chaptersData,
      specificData,
      activeJobsData
    };
  });
  
  console.log(JSON.stringify(result, null, 2));
  await browser.disconnect();
}
run().catch(console.error);
