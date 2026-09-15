import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  
  await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 10000));
  
  const logs = await page.evaluate(() => {
    return document.body.innerText;
  });
  
  const lines = logs.split('\n').filter(Boolean);
  const startupLogs = lines.filter(l => l.includes('Build:') || l.includes('Starting Nox Importer') || l.includes('Build ID:'));
  console.log("Startup Logs:");
  console.log(startupLogs.join('\n'));
  
  await browser.disconnect();
}
run().catch(console.error);
