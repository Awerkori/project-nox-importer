import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function run() {
  const start = '2026-09-16T00:30:00Z';
  const end = '2026-09-16T11:25:00Z';
  
  let failedJobs;
  let attempts = 0;
  while(attempts < 5) {
    const { data, error } = await supabase
      .from('importer_queue')
      .select('id, task_type, source, last_error, attempts, payload')
      .eq('status', 'FAILED')
      .gte('updated_at', start)
      .lte('updated_at', end);
      
    if (!error) {
      failedJobs = data;
      break;
    }
    attempts++;
    await sleep(2000);
  }
  
  if (!failedJobs) {
    console.error("Failed to query after 5 attempts");
    return;
  }
  
  let transient = 0;
  let sourceFailed = 0;
  let invalid = 0;
  
  failedJobs.forEach(job => {
    const err = (job.last_error || '').toLowerCase();
    if (err.includes('timeout') || err.includes('521') || err.includes('522') || err.includes('pgrst') || err.includes('fetch error') || err.includes('network') || err.includes('econnreset') || err.includes('socket')) {
      transient++;
    } else if (err.includes('not found') || err.includes('404') || err.includes('cloudflare') || err.includes('captcha') || err.includes('upstream')) {
      sourceFailed++;
    } else {
      invalid++; 
      console.log(`[INVALID/OTHER] ${job.id} | ${job.task_type} | ${job.last_error}`);
    }
  });
  
  console.log(`TOTAL AUDITED: ${failedJobs.length}`);
  console.log(`TRANSIENT: ${transient}`);
  console.log(`SOURCE FAILED: ${sourceFailed}`);
  console.log(`INVALID: ${invalid}`);
}
run();
