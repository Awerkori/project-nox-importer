import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  
  await page.goto('https://discloudbot.com/dashboard/profile', { waitUntil: 'networkidle2' });
  
  // Try to find an element containing the token. Often it's an input with type="password" or a specific id.
  const html = await page.evaluate(() => document.body.innerHTML);
  const match = html.match(/[a-zA-Z0-9-]{30,}/g); // simple heuristic
  
  // also check local storage
  const ls = await page.evaluate(() => JSON.stringify(window.localStorage));
  
  console.log("LocalStorage:", ls);
  
  await browser.disconnect();
}
run().catch(console.error);
