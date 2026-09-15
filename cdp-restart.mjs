import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  const page = pages.find(p => p.url().includes('1788873398156'));
  
  if (!page) { console.log("No tab found"); return; }
  
  await page.goto('https://discloud.com/dashboard/app/1788873398156', { waitUntil: 'networkidle2' });
  
  console.log("Looking for Reiniciar button...");
  const clicked = await page.evaluate(() => {
    const buttons = Array.from(document.querySelectorAll('button'));
    const btn = buttons.find(b => b.textContent.toLowerCase().includes('reiniciar') || b.textContent.toLowerCase().includes('restart'));
    if (btn) {
      btn.click();
      return true;
    }
    return false;
  });
  
  console.log("Clicked:", clicked);
  if (clicked) {
    await new Promise(r => setTimeout(r, 10000));
  }
  await browser.disconnect();
}
run().catch(console.error);
