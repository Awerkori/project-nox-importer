import puppeteer from 'puppeteer-core';
import fs from 'fs';

async function run() {
  const res = await fetch('http://127.0.0.1:9222/json/version');
  const { webSocketDebuggerUrl } = await res.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  
  await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded' });
  console.log("Listening to logs...");
  
  page.on('console', msg => {
    if (msg.text().includes('TELEMETRY')) {
      fs.appendFileSync('telemetry.log', msg.text() + '\n');
    }
  });

  // Inject an observer in the page to forward logs to console
  await page.evaluate(() => {
    const observer = new MutationObserver(mutations => {
      for (const m of mutations) {
        m.addedNodes.forEach(node => {
          if (node.innerText && node.innerText.includes('TELEMETRY_JOB_STAGED')) {
            console.log(node.innerText);
          }
        });
      }
    });
    observer.observe(document.body, { childList: true, subtree: true });
    
    // Also parse current logs just in case
    const lines = document.body.innerText.split('\n');
    for (const l of lines) {
      if (l.includes('TELEMETRY_JOB_STAGED')) console.log(l);
    }
  });

  // Keep alive for 10 minutes
  await new Promise(r => setTimeout(r, 600000));
  await browser.disconnect();
}
run().catch(console.error);
