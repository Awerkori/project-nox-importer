import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  let supabasePage = pages.find(p => p.url().includes('supabase.com'));
  await supabasePage.bringToFront();
  
  const sql = `
    CREATE OR REPLACE FUNCTION public.debug_acquire_sort() RETURNS jsonb AS $$
    DECLARE
      v_res jsonb;
    BEGIN
      WITH staged_works AS (
        SELECT m.work_id, max(m.chapter_sort_key) as max_staged_sort_key
        FROM public.importer_chapter_mappings m
        WHERE m.status = 'STAGED'
        GROUP BY m.work_id
      ),
      inflight_counts AS (
        SELECT (q_in.payload->>'workId') as work_id, count(*) as active_jobs
        FROM public.importer_queue q_in
        WHERE q_in.status = 'IMPORTING'
        GROUP BY q_in.payload->>'workId'
      ),
      recovery_candidates_raw AS (
        SELECT
          q_rec.id,
          q_rec.task_type,
          q_rec.source,
          85 as effective_priority,
          q_rec.payload,
          q_rec.chapter_sort_key,
          q_rec.created_at,
          q_rec.next_run_at,
          ROW_NUMBER() OVER(PARTITION BY (q_rec.payload->>'workId') ORDER BY q_rec.chapter_sort_key ASC) as rn,
          coalesce(ic.active_jobs, 0) as current_inflight
        FROM staged_works sw
        JOIN public.importer_queue q_rec ON (
          (q_rec.payload->>'workId')::text = sw.work_id::text
          AND coalesce(q_rec.chapter_sort_key, 0) < sw.max_staged_sort_key
          AND q_rec.task_type = 'IMPORT_CHAPTER'
          AND q_rec.status IN ('QUEUED', 'RETRY')
          AND q_rec.next_run_at <= now()
        )
        LEFT JOIN inflight_counts ic ON ic.work_id = (q_rec.payload->>'workId')::text
      ),
      recovery_candidates AS (
        SELECT id, payload->>'workId' as workId, effective_priority, chapter_sort_key, created_at, current_inflight
        FROM recovery_candidates_raw
        WHERE rn = 1
      ),
      normal_candidates AS (
        SELECT
          q_norm.id,
          q_norm.payload->>'workId' as workId,
          q_norm.priority as effective_priority,
          q_norm.chapter_sort_key,
          q_norm.created_at,
          0::bigint as current_inflight
        FROM public.importer_queue q_norm
        JOIN public.importer_sources s ON (s.id = q_norm.source)
        WHERE q_norm.status IN ('QUEUED', 'RETRY')
          AND q_norm.next_run_at <= now()
          AND s.enabled = true
          AND s.status NOT IN ('DISABLED', 'EXCLUDED_BY_POLICY')
          AND q_norm.task_type = 'IMPORT_CHAPTER'
        ORDER BY q_norm.priority DESC, q_norm.next_run_at ASC
        LIMIT 25
      ),
      combined_candidates AS (
        SELECT * FROM recovery_candidates
        UNION ALL
        SELECT * FROM normal_candidates
      )
      SELECT jsonb_agg(row_to_json(cand.*)) INTO v_res
      FROM (
        SELECT * FROM combined_candidates
        ORDER BY effective_priority DESC, current_inflight ASC, created_at ASC
        LIMIT 10
      ) cand;
      
      RETURN v_res;
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
