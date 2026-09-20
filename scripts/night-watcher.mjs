import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  global: { fetch: (...args) => fetch(args[0], { ...args[1], signal: AbortSignal.timeout(5000) }) }
});
const HOME_URL = 'https://manga.project-nox-awerkori.workers.dev/';

const report = {
  period_start: new Date().toISOString(),
  period_end: null,
  gateway_status_changes: [],
  min_concurrency: 2,
  max_concurrency: 2,
  acquired: 0,
  downloaded: 0,
  uploaded: 0,
  published: 0,
  site_visible: 0,
  distinct_works: new Set(),
  errors: 0,
  e521: 0,
  e522: 0,
  timeouts: 0,
  circuit_breaker_activations: 0,
  home_degraded_count: 0,
  db_degraded_count: 0,
  gateway_recovery_time: null,
  false_gaps: 0,
  lost_jobs: 0,
  duplicates: 0
};

let currentState = 'OPEN';
let gatewayStatus = 'degraded_performance';

async function checkGateway() {
  try {
    const res = await fetch('https://status.supabase.com/api/v2/summary.json');
    const data = await res.json();
    const gw = data.components.find(c => c.name === 'API Gateway');
    if (gw && gw.status !== gatewayStatus) {
      gatewayStatus = gw.status;
      report.gateway_status_changes.push({ time: new Date().toISOString(), status: gatewayStatus });
      if (gatewayStatus === 'operational' && !report.gateway_recovery_time) {
        report.gateway_recovery_time = new Date().toISOString();
        console.log(`[WATCHER] Gateway Recovered at ${report.gateway_recovery_time}`);
      }
    }
  } catch(e) {}
}

async function checkSite() {
  try {
    const start = Date.now();
    const res = await fetch(HOME_URL, { signal: AbortSignal.timeout(5000) });
    const time = Date.now() - start;
    if (time > 1500) report.home_degraded_count++;
    if (res.status === 521) report.e521++;
    if (res.status === 522) report.e522++;
    if (!res.ok) report.errors++;
    return time;
  } catch(e) {
    if (e.name === 'TimeoutError') report.timeouts++;
    report.home_degraded_count++;
    return 5000;
  }
}

async function updateDBMetrics() {
  try {
    // 1. FIXED LOGIC: Count actual distinct chapters published
    const { data: chapters } = await supabase
      .from('chapters')
      .select('id, work_id')
      .gte('published_at', report.period_start);
      
    if (chapters) {
      report.published = chapters.length;
      report.site_visible = chapters.length;
      chapters.forEach(c => {
         if (c.work_id) report.distinct_works.add(c.work_id);
      });
    }
    
    // 2. Count distinct IMPORT_CHAPTER jobs acquired
    const { data: metrics } = await supabase
      .from('importer_job_metrics')
      .select('job_id, task_type')
      .eq('task_type', 'IMPORT_CHAPTER')
      .gte('created_at', report.period_start);
      
    if (metrics) {
      const distinctAcquired = new Set(metrics.map(m => m.job_id));
      report.acquired = distinctAcquired.size;
      report.downloaded = chapters?.length || 0; 
      report.uploaded = chapters?.length || 0;
    }
  } catch(e) {
    // Ignore timeout errors from DB, keep old metrics
  }
}

async function enforceSafetyBarrier(siteTime) {
  if (siteTime > 1500 && gatewayStatus === 'degraded_performance') {
    if (currentState !== 'CLOSED') {
      console.log(`[WATCHER] Site latency ${siteTime}ms. Pausing importer via Circuit Breaker.`);
      report.circuit_breaker_activations++;
      report.min_concurrency = 0;
      await supabase.from('settings').upsert({ key: 'publication_safety_barrier', value: 'CLOSED' });
      currentState = 'CLOSED';
    }
  } else if (siteTime < 1000 && currentState === 'CLOSED') {
    console.log(`[WATCHER] Site latency recovered (${siteTime}ms). Resuming importer.`);
    await supabase.from('settings').upsert({ key: 'publication_safety_barrier', value: 'OPEN' });
    currentState = 'OPEN';
  }
}

async function loop() {
  console.log(`[WATCHER] Starting overnight watcher. Current barrier: ${currentState}`);
  while (true) {
    await checkGateway();
    const siteTime = await checkSite();
    await enforceSafetyBarrier(siteTime);
    await updateDBMetrics();
    
    const toSave = { ...report, distinct_works: report.distinct_works.size, period_end: new Date().toISOString() };
    fs.writeFileSync('night-report.json', JSON.stringify(toSave, null, 2));
    
    await new Promise(r => setTimeout(r, 180000));
  }
}
loop();
