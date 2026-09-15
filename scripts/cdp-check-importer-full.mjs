import puppeteer from 'puppeteer-core';
async function run() {
  const res = await fetch('http://127.0.0.1:9222/json/version');
  const { webSocketDebuggerUrl } = await res.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 8000));
  const text = await page.evaluate(() => document.body.innerText);
  const lines = text.split('\n').filter(l => l.trim() && l.includes('{"timestamp"'));
  console.log(`Found ${lines.length} log lines.`);
  const floods = lines.filter(l => l.toLowerCase().includes('flood') || l.toLowerCase().includes('rate limit'));
  console.log(`Flood waits/rate limits: ${floods.length}`);
  floods.forEach(l => console.log(l));
  await browser.disconnect();
}
run().catch(console.error);
