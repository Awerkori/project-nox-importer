import puppeteer from 'puppeteer-core';

async function run() {
  const browser = await puppeteer.launch({ 
    executablePath: '/usr/bin/chromium',
    headless: 'new',
    userDataDir: '/home/awerkori/.config/chromium',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--window-size=1280,800']
  });
  
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'networkidle2' });

  const sql = `EXPLAIN ANALYZE
SELECT q_cand.id
FROM public.importer_queue q_cand
WHERE q_cand.status in ('QUEUED', 'RETRY') 
  AND q_cand.priority >= 100 
  AND q_cand.next_run_at <= now()
  AND q_cand.task_type = 'IMPORT_CHAPTER'
ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 15;`;
  
  await page.evaluate((query) => {
    window.monaco.editor.getModels()[0].setValue(query);
  }, sql);
  
  await new Promise(r => setTimeout(r, 1000));
  
  await page.keyboard.down('Control');
  await page.keyboard.press('Enter');
  await page.keyboard.up('Control');

  console.log("Query submitted. Waiting for results...");
  await new Promise(r => setTimeout(r, 6000));
  
  await page.screenshot({ path: 'dashboard_explain_branch1.png' });
  await browser.close();
  console.log('Done.');
}
run().catch(console.error);
