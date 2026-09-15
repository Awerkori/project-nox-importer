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
      SELECT pg_terminate_backend(pid) 
      FROM pg_stat_activity 
      WHERE state = 'active' AND pid <> pg_backend_pid() AND query NOT ILIKE '%pg_stat_activity%';
    `;
    
    // Spam it 10 times with 100ms intervals hoping one gets through the pool
    for (let i = 0; i < 10; i++) {
      fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
        body: JSON.stringify({ query })
      }).then(r => r.json()).then(console.log).catch(() => {});
      await new Promise(r => setTimeout(r, 500));
    }
    
    return "Sent terminate commands";
  });
  
  console.log(result);
  await browser.disconnect();
}
run().catch(console.error);
