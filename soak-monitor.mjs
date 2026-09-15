import { createClient } from '@supabase/supabase-js';
import { readFileSync, writeFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

async function run() {
  console.log("Starting 30-minute soak test monitoring...");
  const startTime = new Date();
  const endTime = new Date(startTime.getTime() + 30 * 60 * 1000); // 30 mins
  
  let minConcurrency = 999;
  let maxConcurrency = 0;
  let peakRam = 0;
  let maxLag = 0;
  let oomCount = 0;
  let restartCount = 0;
  
  let lastWorkerId = null;
  
  while (new Date() < endTime) {
    const { data } = await supabase
      .from('importer_telemetry')
      .select('*')
      .eq('worker_id', 'discloud-importer-1')
      .order('created_at', { ascending: false })
      .limit(10);
      
    if (data && data.length > 0) {
      for (const row of data) {
        // Only count rows generated during our soak
        if (new Date(row.created_at) < startTime) continue;
        
        if (row.concurrency < minConcurrency) minConcurrency = row.concurrency;
        if (row.concurrency > maxConcurrency) maxConcurrency = row.concurrency;
        if (row.rss_mb > peakRam) peakRam = row.rss_mb;
        if (row.event_loop_lag_ms > maxLag) maxLag = row.event_loop_lag_ms;
        
        // rudimentary restart detection
        if (lastWorkerId && row.id !== lastWorkerId && row.cycle_action === 'STABLE' && row.cycle_reason.includes('1/3 cycles')) {
          restartCount++;
        }
        lastWorkerId = row.id;
      }
    }
    
    // Poll every 30 seconds
    await new Promise(r => setTimeout(r, 30000));
  }
  
  const results = {
    duration: '30 minutes',
    minConcurrency: minConcurrency === 999 ? 0 : minConcurrency,
    maxConcurrency,
    peakRam,
    maxLag,
    restartCount,
    status: 'COMPLETED'
  };
  
  writeFileSync('/home/awerkori/.Projects/project-nox-importer/soak-results.json', JSON.stringify(results, null, 2));
  console.log("Soak test completed. Results saved.");
}

run().catch(err => {
  console.error("Soak failed:", err);
  writeFileSync('/home/awerkori/.Projects/project-nox-importer/soak-results.json', JSON.stringify({ error: err.message }));
});
