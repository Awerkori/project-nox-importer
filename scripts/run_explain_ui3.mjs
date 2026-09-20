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

  const sql = `SELECT indexname FROM pg_indexes WHERE tablename = 'importer_queue';`;
  
  await page.evaluate((query) => {
    window.monaco.editor.getModels()[0].setValue(query);
  }, sql);
  
  await new Promise(r => setTimeout(r, 1000));
  
  await page.keyboard.down('Control');
  await page.keyboard.press('Enter');
  await page.keyboard.up('Control');

  console.log("Query submitted. Waiting for results...");
  await new Promise(r => setTimeout(r, 4000));
  
  const results = await page.evaluate(() => {
    try {
      const grid = document.querySelector('.rdg');
      if (grid) return grid.innerText;
      return "Could not extract results from UI";
    } catch (e) {
      return e.toString();
    }
  });
  
  console.log("Results:\n", results);
  await browser.close();
}
run().catch(console.error);
