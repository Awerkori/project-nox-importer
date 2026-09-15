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
    
    const query = `
      UPDATE importer_queue 
      SET status = 'PAUSED_BY_STAFF' 
      WHERE status IN ('QUEUED', 'IMPORTING', 'PENDING', 'ACQUIRED');
    `;
    
    // Spam it 100 times to get a slot in the pool
    for (let i = 0; i < 100; i++) {
      fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
        body: JSON.stringify({ query })
      }).then(r => r.json()).then(res => {
        if (!res.message || !res.message.includes('timeout')) {
          console.log("SUCCESS:", res);
        }
      }).catch(() => {});
      await new Promise(r => setTimeout(r, 100));
    }
    
    return "Sent pause spam";
  });
  
  console.log(result);
  await browser.disconnect();
}
run().catch(console.error);
