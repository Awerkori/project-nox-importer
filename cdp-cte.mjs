import WebSocket from 'ws';
const wsUrl = 'ws://127.0.0.1:9222/devtools/page/6CAB873D04D27E901617315CBB06DB3F';
const ws = new WebSocket(wsUrl);
let msgId = 1;
function send(method, params = {}) {
  return new Promise((resolve) => {
    const id = msgId++;
    const listener = (data) => {
      const res = JSON.parse(data);
      if (res.id === id) { ws.off('message', listener); resolve(res.result); }
    };
    ws.on('message', listener);
    ws.send(JSON.stringify({ id, method, params }));
  });
}
ws.on('open', async () => {
  const q = `
    CREATE OR REPLACE VIEW test_cte_view AS
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
        ROW_NUMBER() OVER(PARTITION BY (q_rec.payload->>'workId') ORDER BY coalesce(q_rec.chapter_sort_key, 0) ASC) as rn,
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
      SELECT id, payload->>'workId' as workId, current_inflight, chapter_sort_key
      FROM recovery_candidates_raw
      WHERE rn = 1
    )
    SELECT * FROM recovery_candidates;
  `;
  await send('Runtime.evaluate', { expression: `
    (function() {
      const monaco = window.monaco;
      if (monaco) monaco.editor.getModels()[0].setValue(${JSON.stringify(q)});
    })()
  `});
  await new Promise(r => setTimeout(r, 500));
  await send('Runtime.evaluate', { expression: `
    (function() {
      const btns = Array.from(document.querySelectorAll('button'));
      const runBtn = btns.find(b => b.textContent === 'Run' || b.textContent.includes('Run'));
      if (runBtn) runBtn.click();
    })()
  `});
  setTimeout(() => process.exit(0), 2000);
});
