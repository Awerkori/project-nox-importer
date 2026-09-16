import puppeteer from 'puppeteer-core';
import fs from 'fs';

async function run() {
  const browser = await puppeteer.launch({ 
    executablePath: '/usr/bin/chromium',
    headless: 'new',
    userDataDir: '/home/awerkori/.config/chromium',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--window-size=1280,800']
  });
  
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'networkidle2' });

  // Get the full query from src/fix_rpc.ts
  const ts = fs.readFileSync('src/fix_rpc.ts', 'utf8');
  const sqlMatch = ts.match(/BEGIN([\s\S]*?)END;/);
  if (!sqlMatch) {
    console.log("Could not find SQL block");
    process.exit(1);
  }
  let innerSql = sqlMatch[1];
  
  // Replace variables with literal test values to make it EXPLAINable
  innerSql = innerSql.replace(/v_barrier_state/g, "'OPEN'");
  innerSql = innerSql.replace(/v_focus_work_id/g, 'NULL');
  innerSql = innerSql.replace(/p_source/g, 'NULL');
  innerSql = innerSql.replace(/p_task_type/g, 'NULL');
  innerSql = innerSql.replace(/v_focus_request_id/g, 'NULL');
  
  const query = `
EXPLAIN ANALYZE
SELECT q.id
  FROM (
    SELECT cand_batch.id, cand_batch.payload, cand_batch.task_type, cand_batch.chapter_sort_key, cand_batch.priority, cand_batch.created_at
    FROM (
      ( SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority >= 100 AND q_cand.next_run_at <= now()
        AND (q_cand.task_type = 'IMPORT_CHAPTER' OR q_cand.task_type IN ('DISCOVER_WORKS', 'SYNC_WORK'))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 15 )
      UNION ALL
      ( SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority >= 80 AND q_cand.priority < 100 AND q_cand.next_run_at <= now()
        AND (q_cand.task_type = 'IMPORT_CHAPTER' OR q_cand.task_type IN ('DISCOVER_WORKS', 'SYNC_WORK'))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 15 )
      UNION ALL
      ( SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority < 80 AND q_cand.next_run_at <= now()
        AND (q_cand.task_type = 'IMPORT_CHAPTER' OR q_cand.task_type IN ('DISCOVER_WORKS', 'SYNC_WORK'))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 15 )
      UNION ALL
      ( 
        WITH staged_works AS (
          SELECT work_id, MAX(chapter_sort_key) as max_staged_sort_key
          FROM public.importer_chapter_mappings
          WHERE status = 'STAGED'
          GROUP BY work_id
        )
        SELECT q_cand.id, q_cand.task_type, q_cand.priority + 5000 as priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
        FROM staged_works sw
        JOIN public.importer_queue q_cand 
          ON (q_cand.payload->>'workId') = sw.work_id::text
        WHERE q_cand.status in ('QUEUED', 'RETRY') 
          AND q_cand.next_run_at <= now()
          AND q_cand.task_type = 'IMPORT_CHAPTER'
          AND q_cand.chapter_sort_key < sw.max_staged_sort_key
        ORDER BY q_cand.chapter_sort_key ASC LIMIT 20
      )
    ) cand_batch
    ORDER BY
      (
        cand_batch.priority 
        - CASE WHEN cand_batch.payload->>'workId' is not null THEN (
            SELECT count(*) * 1000
            FROM public.importer_queue active_q
            WHERE active_q.status = 'IMPORTING' 
              AND active_q.task_type = 'IMPORT_CHAPTER'
              AND (active_q.payload->>'workId')::text = (cand_batch.payload->>'workId')::text
          ) ELSE 0 END
      ) DESC,
      CASE WHEN cand_batch.chapter_sort_key IS NOT NULL THEN cand_batch.chapter_sort_key ELSE 999999 END ASC,
      cand_batch.created_at ASC
  ) sorted_cands
  JOIN public.importer_queue q ON q.id = sorted_cands.id
  LIMIT 1;
  `;
  
  await page.evaluate((q) => {
    window.monaco.editor.getModels()[0].setValue(q);
  }, query);
  
  await new Promise(r => setTimeout(r, 1000));
  
  await page.keyboard.down('Control');
  await page.keyboard.press('Enter');
  await page.keyboard.up('Control');

  console.log("Query submitted. Waiting for results...");
  await new Promise(r => setTimeout(r, 10000));
  
  await page.screenshot({ path: 'dashboard_explain_full.png' });
  await browser.close();
  console.log('Done.');
}
run().catch(console.error);
