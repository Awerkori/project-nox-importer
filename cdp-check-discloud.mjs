import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  const page = pages.find(p => p.url().includes('1788873398156'));
  
  if (!page) { console.log("No tab found"); return; }
  
  // Go to GitHub integration page or settings
  await page.goto('https://discloud.com/dashboard/app/1788873398156/settings', { waitUntil: 'networkidle2' });
  await new Promise(r => setTimeout(r, 2000));
  
  const text = await page.evaluate(() => document.body.innerText);
  console.log("Settings Text:", text.substring(0, 1000));
  
  await browser.disconnect();
}
run().catch(console.error);
