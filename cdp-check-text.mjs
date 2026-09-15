import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  let supabasePage = pages.find(p => p.url().includes('supabase.com'));
  await supabasePage.bringToFront();
  
  const sql = `
    CREATE OR REPLACE FUNCTION public.check_inflight_text() RETURNS boolean AS $$
    DECLARE
      v_text text;
    BEGIN
      v_text := pg_get_functiondef('public.importer_acquire_job'::regproc);
      RETURN v_text ILIKE '%current_inflight%';
    END;
    $$ LANGUAGE plpgsql;
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
