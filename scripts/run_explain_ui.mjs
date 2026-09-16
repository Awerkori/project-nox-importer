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
WITH staged_works AS (
  SELECT work_id, MAX(chapter_sort_key) as max_staged_sort_key
  FROM public.importer_chapter_mappings
  WHERE status = 'STAGED'
  GROUP BY work_id
)
SELECT q_cand.id
FROM staged_works sw
JOIN public.importer_queue q_cand 
  ON (q_cand.payload->>'workId') = sw.work_id::text
WHERE q_cand.status in ('QUEUED', 'RETRY') 
  AND q_cand.next_run_at <= now()
  AND q_cand.task_type = 'IMPORT_CHAPTER'
  AND q_cand.chapter_sort_key < sw.max_staged_sort_key
ORDER BY q_cand.chapter_sort_key ASC LIMIT 20;`;
  
  await page.evaluate((query) => {
    window.monaco.editor.getModels()[0].setValue(query);
  }, sql);
  
  await new Promise(r => setTimeout(r, 1000));
  
  await page.keyboard.down('Control');
  await page.keyboard.press('Enter');
  await page.keyboard.up('Control');

  console.log("Query submitted. Waiting for results...");
  await new Promise(r => setTimeout(r, 6000));
  
  // Try to extract the results if possible
  const results = await page.evaluate(() => {
    try {
      // The results are in a grid, let's just get the text content of the grid
      const grid = document.querySelector('.rdg');
      if (grid) return grid.innerText;
      
      // Or maybe it's in a pre?
      const pre = document.querySelector('pre');
      if (pre) return pre.innerText;
      
      return "Could not extract results from UI";
    } catch (e) {
      return e.toString();
    }
  });
  
  console.log("Results:\n", results);
  
  await page.screenshot({ path: 'dashboard_explain.png' });
  await browser.close();
  console.log('Done.');
}
run().catch(console.error);
