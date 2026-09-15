import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  let supabasePage = pages.find(p => p.url().includes('supabase.com'));
  await supabasePage.bringToFront();
  
  const sql = `
    DROP FUNCTION IF EXISTS public.importer_acquire_job(text, interval, text, text);
  `;
  
  await supabasePage.evaluate((sqlText) => {
    window.monaco.editor.getModels()[0].setValue(sqlText);
  }, sql);
  await new Promise(r => setTimeout(r, 1000));
  await supabasePage.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    const runBtn = btns.find(b => b.textContent === 'Run' || b.textContent.includes('Run'));
    if (runBtn) runBtn.click();
  });
  await new Promise(r => setTimeout(r, 4000));
  await browser.disconnect();
}
run().catch(console.error);
