import { createClient } from '@supabase/supabase-js';
import fs from 'fs';
import dotenv from 'dotenv';
dotenv.config();

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const LOG_FILE = 'soak-8h-monitor.log';
const METRICS_FILE = 'soak-metrics.jsonl';
const DURATION_MS = 5 * 60 * 60 * 1000;
const INTERVAL_MS = 5 * 60 * 1000;

function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}\n`;
  fs.appendFileSync(LOG_FILE, line);
  console.log(msg);
}

let startPublished = 8525;
let startTime = Date.parse('2026-09-15T18:05:44.952Z');

async function check() {
  try {
    const { count: publishedCount } = await sb.from('chapters').select('*', { count: 'exact', head: true });
    
    // Basic counts
    const { count: qTot } = await sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'QUEUED');
    const { count: qImp } = await sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'IMPORTING');
    const { count: qStg } = await sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'STAGED');
    const { count: qRet } = await sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'RETRY');
    const { count: qCom } = await sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'COMPLETED');

    // Diversity
    const { data: importingJobs } = await sb.from('importer_queue').select('source, payload').eq('status', 'IMPORTING');
    const sources = new Set();
    const works = new Set();
    let manhastroCount = 0;
    
    for (const job of importingJobs || []) {
      sources.add(job.source);
      if (job.payload && job.payload.workId) works.add(job.payload.workId);
      if (job.source === 'manhastro') manhastroCount++;
    }
    
    // Save full snapshot
    const snapshot = {
      time: new Date().toISOString(),
      publishedTotal: publishedCount,
      queued: qTot,
      importing: qImp,
      staged: qStg,
      retry: qRet,
      completed: qCom,
      distinctSources: sources.size,
      distinctWorks: works.size,
      manhastroCount,
      sourceDist: importingJobs?.reduce((acc, j) => { acc[j.source] = (acc[j.source]||0)+1; return acc; }, {}) || {},
    };
    
    fs.appendFileSync(METRICS_FILE, JSON.stringify(snapshot) + '\n');

    const publishedSinceStart = publishedCount - startPublished;
    const minsSinceStart = (Date.now() - startTime) / 60000;
    const avgThroughput = minsSinceStart > 0 ? (publishedSinceStart / minsSinceStart).toFixed(2) : 0;
    
    log(`STATS | QUEUED: ${qTot} | IMPORTING: ${qImp} | STAGED: ${qStg} | RETRY: ${qRet} | PUB: ${publishedCount} | RATE: ${avgThroughput}/min | SOURCES: ${sources.size} | WORKS: ${works.size} | MANHASTRO: ${manhastroCount}`);
  } catch (err) {
    log(`ERROR: ${err.message}`);
  }
}

log('SOAK METRICS UPGRADED');
check();
const timer = setInterval(() => {
  if (Date.now() - startTime > DURATION_MS) {
    log('SOAK COMPLETED');
    clearInterval(timer);
    process.exit(0);
  }
  check();
}, INTERVAL_MS);
