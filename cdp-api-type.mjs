import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  let supabasePage = pages.find(p => p.url().includes('supabase.com'));
  const token = await supabasePage.evaluate(() => window.localStorage.getItem('supabase.dashboard.auth.token'));
  const jwt = JSON.parse(token).access_token;
  
  const res = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
    body: JSON.stringify({ query: `
      SELECT data_type, udt_name 
      FROM information_schema.columns 
      WHERE table_name = 'importer_queue' AND column_name = 'status';
    ` })
  });
  
  const resData = await res.json();
  console.log(resData);
  await browser.disconnect();
}
run().catch(console.error);
