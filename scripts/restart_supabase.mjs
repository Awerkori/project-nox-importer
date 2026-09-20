import puppeteer from 'puppeteer-core';

async function run() {
  const browser = await puppeteer.launch({
    executablePath: '/usr/bin/chromium',
    userDataDir: '/home/awerkori/.config/chromium',
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/settings/general', { waitUntil: 'domcontentloaded' }).catch(e => console.log('Goto timeout ignored'));
  
  await new Promise(r => setTimeout(r, 5000));
  
  const res = await page.evaluate(async () => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    if (!token) return { status: 'No token' };
    const jwt = JSON.parse(token).access_token;
    
    const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/restart', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${jwt}` }
    });
    return { status: response.status, body: await response.text() };
  });
  
  console.log('Result:', res);
  await browser.close();
}
run().catch(console.error);
