import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  global: { fetch: (...args) => fetch(args[0], { ...args[1], signal: AbortSignal.timeout(5000) }) }
});

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function run() {
  const start = '2026-09-16T00:30:00Z';
  const end = '2026-09-16T11:25:00Z';
  
  console.log("Fetching FAILED jobs from night window...");
  let failedJobs = null;
  let attempts = 0;
  while(attempts < 10) {
    try {
      const { data, error } = await supabase
        .from('importer_queue')
        .select('id, last_error')
        .eq('status', 'FAILED')
        .gte('updated_at', start)
        .lte('updated_at', end);
        
      if (!error) {
        failedJobs = data;
        break;
      }
    } catch(e) {}
    attempts++;
    await sleep(2000);
  }
  
  if (!failedJobs) {
    console.error("Could not fetch FAILED jobs. Gateway still degraded?");
    return;
  }
  
  const transientIds = [];
  failedJobs.forEach(job => {
    const err = (job.last_error || '').toLowerCase();
    if (err.includes('timeout') || err.includes('521') || err.includes('522') || err.includes('pgrst') || err.includes('fetch error') || err.includes('network') || err.includes('econnreset') || err.includes('socket')) {
      transientIds.push(job.id);
    }
  });
  
  console.log(`Found ${transientIds.length} transient failed jobs.`);
  if (transientIds.length === 0) return;
  
  console.log("Resetting status to RETRY...");
  // Update in chunks to avoid large payload timeouts
  const chunkSize = 10;
  for (let i = 0; i < transientIds.length; i += chunkSize) {
    const chunk = transientIds.slice(i, i + chunkSize);
    let success = false;
    let updAttempts = 0;
    while (!success && updAttempts < 10) {
      try {
        const { error } = await supabase
          .from('importer_queue')
          .update({
            status: 'RETRY',
            locked_by: null,
            locked_at: null,
            attempts: 0,
            last_error: 'Re-queued automatically after API Gateway recovery'
          })
          .in('id', chunk);
        if (!error) {
          success = true;
          console.log(`Re-queued chunk ${i/chunkSize + 1}`);
        }
      } catch(e) {}
      updAttempts++;
      if (!success) await sleep(2000);
    }
  }
  console.log("Recovery preparation complete! Jobs are RETRY and will be picked up when Importer resumes.");
}
run();
