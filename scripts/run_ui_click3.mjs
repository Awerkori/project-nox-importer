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

  const sql = `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_queue_workid_import ON public.importer_queue ((payload->>'workId')) WHERE status IN ('QUEUED', 'RETRY') AND task_type = 'IMPORT_CHAPTER';`;
  
  // Set the value directly via Monaco API if available
  const success = await page.evaluate((query) => {
    try {
      window.monaco.editor.getModels()[0].setValue(query);
      return true;
    } catch (e) {
      return false;
    }
  }, sql);

  if (!success) {
    console.log("Monaco API failed, falling back to keyboard type...");
    // Fallback if monaco is not global
    const editorSelector = '.monaco-editor textarea';
    await page.waitForSelector(editorSelector);
    await page.click(editorSelector);
    
    await page.keyboard.down('Control');
    await page.keyboard.press('a');
    await page.keyboard.up('Control');
    await page.keyboard.press('Backspace');

    // Type with NO delay, sometimes it's better, or paste using clipboard
    // Let's use document.execCommand('insertText') which simulates paste
    await page.evaluate((query) => {
      const textarea = document.querySelector('.monaco-editor textarea');
      textarea.focus();
      document.execCommand('insertText', false, query);
    }, sql);
  } else {
    console.log("Set value via Monaco API");
  }
  
  await new Promise(r => setTimeout(r, 1000));
  
  await page.keyboard.down('Control');
  await page.keyboard.press('Enter');
  await page.keyboard.up('Control');

  console.log("Query submitted. Waiting for results...");
  await new Promise(r => setTimeout(r, 6000));
  
  await page.screenshot({ path: 'dashboard_after3.png' });
  
  await browser.close();
  console.log('Done. Check dashboard_after3.png');
}
run().catch(console.error);
