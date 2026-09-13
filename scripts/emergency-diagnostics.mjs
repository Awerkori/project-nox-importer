import dotenv from 'dotenv';
dotenv.config();
import { createClient } from '@supabase/supabase-js';

const SUPABASE_MGMT_TOKEN = "sbp_[REDACTED]";
const SUPABASE_PROJECT_REF = "izregkwaqdygwioqzwwo";

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

async function runSql(sql, timeoutMs = 3500) {
  const t0 = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(`https://api.supabase.com/v1/projects/${SUPABASE_PROJECT_REF}/database/query`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${SUPABASE_MGMT_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        query: `SET statement_timeout = '${timeoutMs}ms'; ${sql}`
      }),
      signal: controller.signal
    });
    clearTimeout(timer);
    const duration = Date.now() - t0;
    if (!res.ok) {
      const errText = await res.text();
      return { success: false, duration, error: `HTTP ${res.status}: ${errText}` };
    }
    const data = await res.json();
    return { success: true, duration, data };
  } catch (err) {
    clearTimeout(timer);
    const duration = Date.now() - t0;
    return {
      success: false,
      duration,
      isTimeout: err.name === 'AbortError' || err.message.includes('timeout'),
      error: err.message
    };
  }
}

async function safeStep(name, fn, timeoutMs = 3500) {
  const t0 = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await Promise.race([
      fn(controller.signal),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error(`TIMEOUT (${timeoutMs}ms)`)), timeoutMs)
      )
    ]);
    clearTimeout(timer);
    const duration = Date.now() - t0;
    return { success: true, duration, data: res };
  } catch (err) {
    clearTimeout(timer);
    const duration = Date.now() - t0;
    console.log(`TIMEOUT:\n  qual query/operação: ${name}\n  duração: ${duration}ms\n  estado: ${err.message}`);
    return { success: false, duration, error: err.message };
  }
}

async function main() {
  console.log('====================================================');
  console.log('PROJECT NOX — FAIL-FAST EMERGENCY DIAGNOSTICS');
  console.log(`Timestamp: ${new Date().toISOString()}`);
  console.log('====================================================\n');

  // 1. Check PostgreSQL Active Queries, Locks, and Blocker PIDs
  console.log('--- 1. ACTIVE POSTGRESQL ACTIVITY & BLOCKERS ---');
  const activitySql = `
    SELECT
      pid,
      state,
      wait_event_type,
      wait_event,
      now() - query_start as duration,
      pg_blocking_pids(pid) as blocker_pids,
      left(query, 120) as query_snippet
    FROM pg_stat_activity
    WHERE state != 'idle'
      AND query NOT LIKE '%pg_stat%'
      AND query NOT LIKE '%statement_timeout%'
    ORDER BY duration DESC NULLS LAST
    LIMIT 10;
  `;
  const actRes = await runSql(activitySql, 3500);
  if (actRes.success) {
    console.log(`Active non-idle queries (${actRes.duration}ms):`, actRes.data);
  } else {
    console.log(`TIMEOUT:\n  qual query/operação: pg_stat_activity\n  duração: ${actRes.duration}ms\n  estado: ${actRes.error}`);
  }

  // 2. Publication Safety Barrier Setting in Database
  console.log('\n--- 2. PUBLICATION SAFETY BARRIER STATE ---');
  const barrierRes = await safeStep('get_barrier_setting', async () => {
    const { data, error } = await sb.from('settings').select('key, value').eq('key', 'publication_safety_barrier').maybeSingle();
    if (error) throw error;
    return data;
  }, 3000);
  console.log('Barrier in settings:', barrierRes.success ? barrierRes.data : barrierRes.error);

  // 3. System Health RPC
  console.log('\n--- 3. SYSTEM HEALTH METRICS ---');
  const healthRes = await safeStep('admin_get_system_health', async () => {
    const { data, error } = await sb.rpc('admin_get_system_health');
    if (error) throw error;
    return data;
  }, 4000);

  if (healthRes.success && healthRes.data) {
    const h = healthRes.data;
    console.log('DB Connections:', h.database?.current_connections, '/', h.database?.max_connections, `(Active: ${h.database?.active_connections}, Idle: ${h.database?.idle_connections})`);
    console.log('Waiting locks:', h.database?.waiting_locks, 'Deadlocks:', h.database?.deadlocks);
    console.log('Queue counts:', h.counts);
    console.log('Sources:', h.sources);
  } else {
    console.log(`System health metrics unavailable: ${healthRes.error}`);
  }

  // 4. Stalled Staged Mappings Analysis
  console.log('\n--- 4. STAGED CHAPTERS BACKLOG & GAPS ---');
  const stagedSql = `
    SELECT
      m.work_id,
      w.title,
      count(*) as total_staged,
      min(m.chapter_number) as min_staged_chapter,
      max(m.chapter_number) as max_staged_chapter
    FROM public.importer_chapter_mappings m
    LEFT JOIN public.works w ON w.id = m.work_id
    WHERE m.status = 'STAGED'
    GROUP BY m.work_id, w.title
    ORDER BY total_staged DESC
    LIMIT 5;
  `;
  const stagedRes = await runSql(stagedSql, 3500);
  if (stagedRes.success) {
    console.log('Top works with STAGED chapters awaiting publication:');
    console.table(stagedRes.data);
  } else {
    console.log(`TIMEOUT:\n  qual query/operação: staged_backlog_summary\n  duração: ${stagedRes.duration}ms\n  estado: ${stagedRes.error}`);
  }

  // 5. Inspect specific work 14941714 (or the top staged work)
  const topWorkId = stagedRes.success && stagedRes.data?.[0]?.work_id
    ? stagedRes.data[0].work_id
    : '14941714-69b9-4183-bf65-6c4ca27dbea7';

  console.log(`\n--- 5. FORENSIC CHAPTER DETAIL FOR TOP STALLED WORK (${topWorkId}) ---`);
  const workDetailSql = `
    SELECT
      m.chapter_number,
      m.chapter_sort_key,
      m.status as mapping_status,
      m.is_gap,
      c.published_at,
      q.status as queue_status,
      q.next_run_at as queue_next_run
    FROM public.importer_chapter_mappings m
    LEFT JOIN public.chapters c ON c.id = m.chapter_id
    LEFT JOIN public.importer_queue q ON (
      q.task_type = 'IMPORT_CHAPTER'
      AND (q.payload->>'workId')::text = m.work_id::text
      AND q.chapter_sort_key = m.chapter_sort_key
      AND q.status IN ('QUEUED', 'RETRY', 'IMPORTING')
    )
    WHERE m.work_id = '${topWorkId}'
    ORDER BY m.chapter_sort_key ASC
    LIMIT 20;
  `;
  const detailRes = await runSql(workDetailSql, 3500);
  if (detailRes.success) {
    console.table(detailRes.data);
  } else {
    console.log(`TIMEOUT:\n  qual query/operação: work_detail_sql\n  duração: ${detailRes.duration}ms\n  estado: ${detailRes.error}`);
  }

  // 6. Test Barrier RPC directly for the first staged chapter of this work
  if (detailRes.success && detailRes.data?.length > 0) {
    const firstStaged = detailRes.data.find(r => r.mapping_status === 'STAGED');
    if (firstStaged) {
      console.log(`\n--- 6. TESTING BARRIER RPC ON FIRST STAGED CHAPTER (${firstStaged.chapter_number}) ---`);
      const barrierTestSql = `
        SELECT * FROM public.importer_check_publication_barrier('${topWorkId}'::uuid, ${firstStaged.chapter_sort_key}::numeric);
      `;
      const barrierTestRes = await runSql(barrierTestSql, 3500);
      if (barrierTestRes.success) {
        console.log('Barrier evaluation result:', barrierTestRes.data);
      } else {
        console.log(`TIMEOUT:\n  qual query/operação: test_barrier_rpc\n  duração: ${barrierTestRes.duration}ms\n  estado: ${barrierTestRes.error}`);
      }
    }
  }

  // 7. Test Web Latency (Fail-fast 3000ms)
  console.log('\n--- 7. LIVE WEB LATENCIES ---');
  const webUrls = [
    'https://manga.project-nox-awerkori.workers.dev/',
    'https://manga.project-nox-awerkori.workers.dev/catalogo'
  ];
  for (const u of webUrls) {
    const t0 = Date.now();
    try {
      const resp = await fetch(u, {
        headers: { 'User-Agent': 'Mozilla/5.0 ProjectNox-EmergencyDiag/1.0' },
        signal: AbortSignal.timeout(3000)
      });
      console.log(`  [${resp.status}] ${u} -> ${Date.now() - t0}ms`);
    } catch (e) {
      console.log(`TIMEOUT:\n  qual query/operação: HTTP ${u}\n  duração: ${Date.now() - t0}ms\n  estado: ${e.message}`);
    }
  }

  console.log('\n====================================================');
  console.log('DIAGNOSTIC RUN COMPLETE');
  console.log('====================================================');
  process.exit(0);
}

main().catch(err => {
  console.error('Fatal diagnostic failure:', err);
  process.exit(1);
});
