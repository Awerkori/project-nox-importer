import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  const page = pages.find(p => p.url().includes('1788873398156'));
  
  if (!page) { console.log("No tab found"); return; }
  
  await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 4000));
  
  // Extract text from terminal/logs area
  const logs = await page.evaluate(() => {
    const el = document.querySelector('code, pre, .logs, .terminal');
    return el ? el.innerText : document.body.innerText.substring(0, 1000);
  });
  
  console.log("LOGS:");
  console.log(logs.slice(-500)); // Print last 500 chars
  
  await browser.disconnect();
}
run().catch(console.error);
