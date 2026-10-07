import https from 'node:https';
import { chromium } from '@playwright/test';

const URL = 'https://manga.project-nox-awerkori.workers.dev/obra/imperador-magico';

async function measureRaw() {
  const t0 = performance.now();
  return new Promise((resolve) => {
    https.get(URL, { headers: { 'User-Agent': 'NoxAuditor/1.0' } }, (res) => {
      const ttfb = Math.round(performance.now() - t0);
      let html = '';
      res.on('data', chunk => html += chunk);
      res.on('end', () => {
        const transferMs = Math.round(performance.now() - t0);
        const chapterRows = (html.match(/class="chapter-item"/g) || []).length;
        resolve({
          statusCode: res.statusCode,
          ttfb,
          transferMs,
          htmlBytes: Buffer.byteLength(html, 'utf8'),
          chapterRowsRendered: chapterRows
        });
      });
    });
  });
}

async function measureBrowser() {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1280, height: 800 } });

  const t0 = performance.now();
  const response = await page.goto(URL, { waitUntil: 'domcontentloaded' });
  const domContentLoadedMs = Math.round(performance.now() - t0);

  await page.waitForLoadState('networkidle');
  const networkIdleMs = Math.round(performance.now() - t0);

  const metrics = await page.evaluate(() => {
    const domNodeCount = document.querySelectorAll('*').length;
    const chapterNodes = document.querySelectorAll('.chapter-item').length;
    
    // Performance navigation timing
    const nav = performance.getEntriesByType('navigation')[0];
    const serverRenderMs = nav ? Math.round(nav.responseStart - nav.requestStart) : 0;
    const transferMs = nav ? Math.round(nav.responseEnd - nav.responseStart) : 0;
    const browserParseMs = nav ? Math.round(nav.domInteractive - nav.responseEnd) : 0;
    const domCompleteMs = nav ? Math.round(nav.domComplete - nav.domInteractive) : 0;

    // Paint timings
    let fcp = 0;
    for (const entry of performance.getEntriesByType('paint')) {
      if (entry.name === 'first-contentful-paint') fcp = Math.round(entry.startTime);
    }

    return {
      domNodeCount,
      chapterNodes,
      serverRenderMs,
      transferMs,
      browserParseMs,
      domCompleteMs,
      fcp
    };
  });

  await browser.close();
  return { domContentLoadedMs, networkIdleMs, ...metrics };
}

async function main() {
  console.log("Measuring raw transfer...");
  const raw = await measureRaw();
  console.log("Raw transfer metrics:", raw);

  console.log("Measuring browser rendering & hydration...");
  const browser = await measureBrowser();
  console.log("Browser metrics:", browser);
}

main().catch(console.error);
