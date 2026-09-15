import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  const page = pages.find(p => p.url().includes('1788873398156'));
  
  await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 10000));
  
  const logs = await page.evaluate(() => {
    // DIScloud logs usually are in a div with a specific class or we can just grab the whole text
    return document.body.innerText;
  });
  
  const lines = logs.split('\n');
  const noxLogs = lines.filter(l => l.includes('Project Nox Importer daemon'));
  console.log("Found log lines:", noxLogs);
  
  await browser.disconnect();
}
run().catch(console.error);
