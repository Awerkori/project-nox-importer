import puppeteer from 'puppeteer-core';

async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 4000));
  
  const res = await page.evaluate(async () => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    if (!token) return { error: "No token" };
    const jwt = JSON.parse(token).access_token;
    
    // Check all triggers
    const query = "SELECT tgname, pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname LIKE '%latest_chapter%'";
    const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query })
    });
    return { status: response.status, body: await response.text() };
  });
  
  console.log(res.body);
  
  const res2 = await page.evaluate(async () => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    const jwt = JSON.parse(token).access_token;
    
    // Get get_recent_releases source
    const query = "SELECT prosrc FROM pg_proc WHERE proname = 'get_recent_releases'";
    const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query })
    });
    return { status: response.status, body: await response.text() };
  });
  
  console.log("get_recent_releases:");
  console.log(res2.body);

  await page.close();
  await browser.disconnect();
}
run().catch(console.error);
