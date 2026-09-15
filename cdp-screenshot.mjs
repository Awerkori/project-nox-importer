import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  
  await page.goto('https://discloudbot.com/dashboard/apps/project-nox-importer', { waitUntil: 'networkidle0' });
  await page.screenshot({ path: '/home/awerkori/.Projects/project-nox-importer/discloud-app.png' });
  
  await browser.disconnect();
}
run().catch(console.error);
