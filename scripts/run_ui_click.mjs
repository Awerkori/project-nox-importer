import puppeteer from 'puppeteer-core';

async function run() {
  const browser = await puppeteer.launch({ 
    executablePath: '/usr/bin/chromium',
    headless: 'new',
    userDataDir: '/home/awerkori/.config/chromium',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--window-size=1280,800']
  });
  
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'networkidle0' });

  // Type the SQL into the editor
  // The Monaco editor can be tricky to type into. We can evaluate JS to set its value, or use page.keyboard.
  
  // Easiest way to set Monaco value is to find the model and setValue
  await page.evaluate(() => {
    // Monaco editor exposes monaco global in some setups, or we can use the DOM.
    // Instead of hacking Monaco, let's just focus the text area and paste.
  });
  
  // Let's just focus and type
  const editorSelector = '.monaco-editor textarea';
  await page.waitForSelector(editorSelector);
  await page.click(editorSelector);
  
  // Select all and delete just in case
  await page.keyboard.down('Control');
  await page.keyboard.press('A');
  await page.keyboard.up('Control');
  await page.keyboard.press('Backspace');

  const sql = `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_queue_workid_import ON public.importer_queue ((payload->>'workId')) WHERE status IN ('QUEUED', 'RETRY') AND task_type = 'IMPORT_CHAPTER';`;
  await page.keyboard.type(sql, { delay: 10 });
  
  // Wait a bit
  await new Promise(r => setTimeout(r, 1000));
  
  // Click Run button
  const runBtnSelector = 'button:has-text("Run")';
  const runBtnXPath = "//button[contains(., 'Run') or contains(., 'RUN')]";
  const buttons = await page.$x(runBtnXPath);
  if (buttons.length > 0) {
    await buttons[0].click();
  } else {
    // fallback to ctrl+enter
    await page.keyboard.down('Control');
    await page.keyboard.press('Enter');
    await page.keyboard.up('Control');
  }

  // Wait for results
  console.log("Query submitted. Waiting for results...");
  await new Promise(r => setTimeout(r, 5000));
  
  await page.screenshot({ path: 'dashboard_after.png' });
  
  await browser.close();
  console.log('Done. Check dashboard_after.png');
}
run().catch(console.error);
