import puppeteer from 'puppeteer-core';

const sql = `
ALTER TYPE work_kind ADD VALUE IF NOT EXISTS 'UNKNOWN';
ALTER TYPE work_status ADD VALUE IF NOT EXISTS 'UNKNOWN';
`;

async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  let supabasePage = pages.find(p => p.url().includes('supabase.com'));
  
  const token = await supabasePage.evaluate(() => {
    return window.localStorage.getItem('supabase.dashboard.auth.token');
  });
  
  const parsedToken = JSON.parse(token);
  const jwt = parsedToken.access_token;
  
  const res = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
    body: JSON.stringify({ query: sql })
  });
  
  console.log(await res.text());
  await browser.disconnect();
}
run();
