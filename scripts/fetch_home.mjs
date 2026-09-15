import puppeteer from 'puppeteer-core';
async function run() {
  const res = await fetch('http://127.0.0.1:9222/json/version');
  const { webSocketDebuggerUrl } = await res.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  await page.goto('https://noxmangas.com/', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 5000));
  
  const results = await page.evaluate(() => {
    const section = document.querySelector('#lancamentos');
    if (!section) return { error: 'No section' };
    
    const grid = section.querySelector('.releases-grid');
    if (!grid) return { error: 'No grid' };
    
    const cards = grid.querySelectorAll('.release-row-card');
    const bounds = section.getBoundingClientRect();
    
    return {
      cardsCount: cards.length,
      visibleCards: Array.from(cards).map(c => {
        const title = c.querySelector('.work-link')?.innerText;
        const style = window.getComputedStyle(c);
        return { title, display: style.display, visibility: style.visibility, height: style.height };
      }),
      sectionHeight: bounds.height,
      emptySpaceBelow: window.innerHeight - bounds.bottom
    };
  });
  
  console.log(JSON.stringify(results, null, 2));
  await browser.disconnect();
}
run().catch(console.error);
