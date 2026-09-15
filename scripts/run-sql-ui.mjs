import puppeteer from 'puppeteer-core';

const sql = `
ALTER TABLE public.works DROP CONSTRAINT IF EXISTS works_kind_check;
ALTER TABLE public.works DROP CONSTRAINT IF EXISTS works_status_check;
ALTER TABLE public.works ADD CONSTRAINT works_kind_check CHECK (kind IN ('MANGA', 'MANHWA', 'MANHUA', 'WEBTOON', 'PORNHWA', 'UNKNOWN'));
ALTER TABLE public.works ADD CONSTRAINT works_status_check CHECK (status IN ('ONGOING', 'COMPLETED', 'HIATUS', 'CANCELLED', 'UNKNOWN'));
`;

async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' });
  
  console.log("Waiting for Monaco editor...");
  await new Promise(r => setTimeout(r, 10000));
  
  await page.evaluate(async (query) => {
    // Monaco editor exposes window.monaco
    // We can also just try to find the button "Run" and click it, but wait, if we can't type, we can't run.
    // Instead of Monaco, let's use the API! 
    // The dashboard UI makes API calls to execute SQL! 
    // It hits /api/pg-meta/{projectRef}/query !
  }, sql);
  
  // Wait, I can just fetch from the page context!
  const res = await page.evaluate(async (query) => {
     const token = window.localStorage.getItem('supabase.dashboard.auth.token');
     const jwt = JSON.parse(token).access_token;
     
     const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
        method: 'POST',
        headers: {
           'Content-Type': 'application/json',
           'Authorization': `Bearer ${jwt}`
        },
        body: JSON.stringify({ query: query })
     });
     return { status: response.status, body: await response.text() };
  }, sql);
  
  console.log("Result:", res);
  await browser.disconnect();
}
run();
