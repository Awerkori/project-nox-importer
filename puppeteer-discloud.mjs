import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  
  let discloudPage = pages.find(p => p.url().includes('discloud.com/dashboard'));
  if (!discloudPage) {
     console.log("No DIScloud page found");
     process.exit(1);
  }
  await discloudPage.bringToFront();
  
  const text = await discloudPage.evaluate(() => document.body.innerText);
  console.log("PAGE TEXT:", text.substring(0, 500));
  await browser.disconnect();
}
run().catch(console.error);
