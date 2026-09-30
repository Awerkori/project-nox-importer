import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  
  // Go to DIScloud dashboard (since the token might be in local storage there)
  await page.goto('https://discloud.com/dashboard/apps/760f38ba51062b3294025f82/logs', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 8000));
  
  // Actually, wait, let's use the DIScloud API if we have the token!
const discloudToken = process.env.DISCLOUD_TOKEN || '';
if (!discloudToken) throw new Error('DISCLOUD_TOKEN is required');
  
}
run();
