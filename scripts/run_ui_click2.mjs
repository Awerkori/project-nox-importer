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

  const editorSelector = '.monaco-editor textarea';
  await page.waitForSelector(editorSelector);
  await page.click(editorSelector);
  
  await page.keyboard.down('Control');
  await page.keyboard.press('a');
  await page.keyboard.up('Control');
  await page.keyboard.press('Backspace');

  const sql = `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_queue_workid_import ON public.importer_queue ((payload->>'workId')) WHERE status IN ('QUEUED', 'RETRY') AND task_type = 'IMPORT_CHAPTER';`;
  await page.keyboard.type(sql, { delay: 10 });
  
  await new Promise(r => setTimeout(r, 1000));
  
  await page.keyboard.down('Control');
  await page.keyboard.press('Enter');
  await page.keyboard.up('Control');

  console.log("Query submitted via Ctrl+Enter. Waiting for results...");
  await new Promise(r => setTimeout(r, 5000));
  
  await page.screenshot({ path: 'dashboard_after.png' });
  
  await browser.close();
  console.log('Done. Check dashboard_after.png');
}
run().catch(console.error);
