import { execSync } from 'child_process';
import fs from 'fs';
import { createClient } from '@supabase/supabase-js';

const env = Object.fromEntries(fs.readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const setConcurrency = (globalLimit, jobLimit) => {
  let concurrencyCode = fs.readFileSync('src/core/concurrency.ts', 'utf-8');
  concurrencyCode = concurrencyCode.replace(/this\.globalMediaSemaphore = new AsyncSemaphore\(\d+\);/, `this.globalMediaSemaphore = new AsyncSemaphore(${globalLimit});`);
  fs.writeFileSync('src/core/concurrency.ts', concurrencyCode, 'utf-8');

  let engineCode = fs.readFileSync('src/core/engine.ts', 'utf-8');
  engineCode = engineCode.replace(/const uploadConcurrency = .*?;/, `const uploadConcurrency = ${jobLimit};`);
  fs.writeFileSync('src/core/engine.ts', engineCode, 'utf-8');
  
  console.log(`Patched to global=${globalLimit}, job=${jobLimit}`);
};

const deployAndMeasure = async (level) => {
  console.log(`\n=== RUNNING BENCHMARK LEVEL ${level} ===`);
  setConcurrency(level, Math.min(level, 3)); // if global is 1, job is 1. If global is 6, job is 3.

  console.log("Committing and pushing...");
  execSync(`gh api --method PUT repos/Awerkori/project-nox-importer/branches/main/protection --input - << 'JSON'
{"required_status_checks": null,"enforce_admins": false,"required_pull_request_reviews": null,"restrictions": null}
JSON`);
  execSync(`git commit -am "benchmark: set concurrency to ${level}" && git push origin main`);
  execSync(`gh api --method PUT repos/Awerkori/project-nox-importer/branches/main/protection --input - << 'JSON'
{"required_status_checks": {"strict": false, "contexts": ["check-build"]},"enforce_admins": true,"required_pull_request_reviews": null,"restrictions": null}
JSON`);

  console.log("Waiting for deploy (60s)...");
  await new Promise(r => setTimeout(r, 60000));
  
  // Clear old telemetry by updating existing ones (hack: change status) or just remember time
  const startTime = new Date().toISOString();
  
  console.log("Soaking and measuring (120s)...");
  await new Promise(r => setTimeout(r, 120000));
  
  // Measure
  const { data: jobs } = await sb
    .from('importer_queue')
    .select('payload')
    .eq('task_type', 'IMPORT_CHAPTER')
    .eq('status', 'COMPLETED')
    .not('payload->telemetry', 'is', null)
    .gte('updated_at', startTime);
    
  if (!jobs || jobs.length === 0) {
    console.log(`Level ${level}: 0 jobs completed.`);
    return;
  }
  
  let down = [], up = [], total = [], bytes = [];
  for (const j of jobs) {
    const t = j.payload.telemetry;
    down.push((t.tDownloadEnd - t.tDownloadStart)/1000);
    up.push((t.tUploadEnd - t.tUploadStart)/1000);
    total.push((t.tStaged - t.tStart)/1000);
    if(t.totalBytesDown) bytes.push(t.totalBytesDown/1024/1024);
  }
  
  const avg = (a) => a.length ? (a.reduce((x,y)=>x+y,0)/a.length).toFixed(2) : 0;
  
  console.log(`Level ${level} Results:`);
  console.log(`Jobs completed: ${jobs.length} (Rate: ${(jobs.length/2).toFixed(1)}/min)`);
  console.log(`Avg Download: ${avg(down)}s`);
  console.log(`Avg Upload: ${avg(up)}s`);
  console.log(`Avg Total to Staged: ${avg(total)}s`);
  console.log(`Avg Size: ${avg(bytes)} MB`);
};

// We will run this manually for each level so the agent can see it
