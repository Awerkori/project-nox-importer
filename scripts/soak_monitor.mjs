import { createClient } from '@supabase/supabase-js';
import fs from 'fs';
import dotenv from 'dotenv';
dotenv.config();

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const LOG_FILE = 'soak-8h-monitor.log';
const DURATION_MS = 5 * 60 * 60 * 1000; // 5 hours
const INTERVAL_MS = 5 * 60 * 1000; // 5 mins

function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}\n`;
  fs.appendFileSync(LOG_FILE, line);
}

let startPublished = 8525;
let startTime = Date.parse('2026-09-15T18:05:44.952Z');
let lastPublished = 0;

async function check() {
  try {
    const { count: publishedCount } = await sb.from('chapters').select('*', { count: 'exact', head: true });
    
    const { count: qTot } = await sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'QUEUED');
    const { count: qImp } = await sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'IMPORTING');
    const { count: qStg } = await sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'STAGED');
    const { count: qRet } = await sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'RETRY');

    const publishedSinceStart = publishedCount - startPublished;
    const minsSinceStart = (Date.now() - startTime) / 60000;
    const avgThroughput = minsSinceStart > 0 ? (publishedSinceStart / minsSinceStart).toFixed(2) : 0;
    
    log(`STATS | QUEUED: ${qTot} | IMPORTING: ${qImp} | STAGED: ${qStg} | RETRY: ${qRet} | PUBLISHED_TOTAL: ${publishedCount} | AVG_RATE: ${avgThroughput}/min`);
    lastPublished = publishedCount;
  } catch (err) {
    log(`ERROR: ${err.message}`);
  }
}

log('SOAK RESUMED');
check();
const timer = setInterval(() => {
  if (Date.now() - startTime > DURATION_MS) {
    log('SOAK COMPLETED');
    clearInterval(timer);
    process.exit(0);
  }
  check();
}, INTERVAL_MS);
