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
  await page.waitForSelector(editorSelector, { timeout: 30000 });
  await page.click(editorSelector);
  
  await page.keyboard.down('Control');
  await page.keyboard.press('a');
  await page.keyboard.up('Control');
  await page.keyboard.press('Backspace');

  const sql = `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_queue_prio_run_all ON public.importer_queue (priority DESC, next_run_at ASC) WHERE status IN ('QUEUED', 'RETRY');`;
  
  // Use evaluation to paste to avoid dropped characters
  await page.evaluate((query) => {
    const textarea = document.querySelector('.monaco-editor textarea');
    textarea.focus();
    document.execCommand('insertText', false, query);
  }, sql);
  
  await new Promise(r => setTimeout(r, 1000));
  
  await page.keyboard.down('Control');
  await page.keyboard.press('Enter');
  await page.keyboard.up('Control');

  console.log("Query submitted. Waiting for results...");
  await new Promise(r => setTimeout(r, 10000));
  
  await page.screenshot({ path: 'dashboard_after_final.png' });
  await browser.close();
  console.log('Done.');
}
run().catch(console.error);
