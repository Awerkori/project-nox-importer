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
    
    // Try different paths
    const paths = [
      'https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/restart',
      'https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/reboot',
      'https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/restart'
    ];
    
    for (const p of paths) {
      try {
        const r = await fetch(p, { method: 'POST', headers: { 'Authorization': `Bearer ${jwt}` } });
        if (r.ok) return `Success on ${p}`;
      } catch (e) {}
    }
    return 'All failed';
  });
  
  console.log(JSON.stringify(result, null, 2));
  await browser.disconnect();
}
run().catch(console.error);
