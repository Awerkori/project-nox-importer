import puppeteer from 'puppeteer-core';

const sql = `
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_queue_workid_import 
ON public.importer_queue ((payload->>'workId')) 
WHERE status IN ('QUEUED', 'RETRY') AND task_type = 'IMPORT_CHAPTER';
`;

async function run() {
  console.log("Launching Chromium...");
  const browser = await puppeteer.launch({ 
    executablePath: '/usr/bin/chromium',
    headless: 'new',
    userDataDir: '/home/awerkori/.config/chromium',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  
  console.log("Navigating to Supabase Dashboard...");
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 8000));
  
  console.log("Executing SQL...");
  const res = await page.evaluate(async (query) => {
    const tokenStr = window.localStorage.getItem('supabase.dashboard.auth.token');
    if (!tokenStr) return { status: 401, body: "No token in localStorage" };
    const jwt = JSON.parse(tokenStr).access_token;
    const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query })
    });
    return { status: response.status, body: await response.text() };
  }, sql);
  
  console.log("Migration result:", res.status, res.body.slice(0, 500));
  await browser.close();
}
run().catch(console.error);
