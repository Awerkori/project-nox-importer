import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  const page = pages.find(p => p.url().includes('1788873398156'));
  
  if (!page) { console.log("No tab found"); return; }
  
  await page.goto('https://discloud.com/dashboard/app/1788873398156', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 3000));
  
  console.log("Looking for buttons...");
  const clicked = await page.evaluate(() => {
    const buttons = Array.from(document.querySelectorAll('button, a'));
    const names = buttons.map(b => b.textContent.trim().toLowerCase());
    console.log(names);
    
    // Sometimes on discloud it's an icon. Let's look for standard button texts:
    const btn = buttons.find(b => {
      const t = b.textContent.toLowerCase();
      return t.includes('reiniciar') || t.includes('restart') || t.includes('commit') || t.includes('deploy') || t.includes('atualizar');
    });
    
    if (btn) {
      btn.click();
      return btn.textContent.trim();
    }
    return false;
  });
  
  console.log("Clicked:", clicked);
  if (clicked) {
    await new Promise(r => setTimeout(r, 5000));
  }
  await browser.disconnect();
}
run().catch(console.error);
