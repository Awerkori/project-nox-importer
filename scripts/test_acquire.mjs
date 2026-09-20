import puppeteer from 'puppeteer-core';
async function run() {
  const browser = await puppeteer.launch({
    executablePath: '/usr/bin/chromium',
    userDataDir: '/home/awerkori/.config/chromium',
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' }).catch(e => {});
  await new Promise(r => setTimeout(r, 5000));
  
  const res = await page.evaluate(async () => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    const jwt = JSON.parse(token).access_token;
    const r1 = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: `SELECT * FROM importer_acquire_job('test-worker', '5 minutes'::interval);` })
    });
    return await r1.text();
  });
  console.log(res);
  await browser.close();
}
run();
