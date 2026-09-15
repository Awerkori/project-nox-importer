import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  
  await page.goto('https://discloudbot.com/dashboard', { waitUntil: 'networkidle2' });
  
  const token = await page.evaluate(() => {
    return window.localStorage.getItem('token') || window.localStorage.getItem('access_token') || document.cookie;
  });
  
  console.log("Token:", token);
  await browser.disconnect();
}
run().catch(console.error);
