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

  const sql = `
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_queue_prio_run_all ON public.importer_queue (priority DESC, next_run_at ASC) WHERE status IN ('QUEUED', 'RETRY');
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_queue_workid_all ON public.importer_queue ((payload->>'workId')) WHERE status IN ('QUEUED', 'RETRY');
`;
  
  await page.waitForFunction(() => !!window.monaco, { timeout: 10000 }).catch(()=>null);
  
  await page.evaluate((query) => {
    window.monaco.editor.getModels()[0].setValue(query);
  }, sql);
  
  await new Promise(r => setTimeout(r, 1000));
  
  await page.keyboard.down('Control');
  await page.keyboard.press('Enter');
  await page.keyboard.up('Control');

  console.log("Query submitted. Waiting for results...");
  await new Promise(r => setTimeout(r, 10000));
  
  await browser.close();
  console.log('Done.');
}
run().catch(console.error);
