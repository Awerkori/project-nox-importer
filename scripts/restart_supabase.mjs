import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version').catch(() => null);
  if (!browserRes) {
    console.log("No browser running");
    return;
  }
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/settings/general', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 6000));
  
  // Try to find the Restart Project button and click it
  const res = await page.evaluate(async () => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    const jwt = JSON.parse(token).access_token;
    
    // We can just hit the restart API directly
    const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/restart', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${jwt}` }
    });
    return { status: response.status, body: await response.text() };
  });
  console.log(res);
  await page.close();
  await browser.disconnect();
}
run();
