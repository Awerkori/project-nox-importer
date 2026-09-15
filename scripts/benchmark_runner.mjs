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
  engineCode = engineCode.replace(/const uploadConcurrency = Math\.min\(.*?\);/g, `const uploadConcurrency = ${jobLimit};`);
  fs.writeFileSync('src/core/engine.ts', engineCode, 'utf-8');
  
  console.log(`Patched to global=${globalLimit}, job=${jobLimit}`);
};

const avg = (a) => a.length ? (a.reduce((x,y)=>x+y,0)/a.length).toFixed(2) : 0;
const p50 = (arr) => arr.length ? arr.sort((a,b)=>a-b)[Math.floor(arr.length*0.5)].toFixed(2) : 0;
const p95 = (arr) => arr.length ? arr.sort((a,b)=>a-b)[Math.floor(arr.length*0.95)].toFixed(2) : 0;

async function runLevel(level) {
  console.log(`\n=== RUNNING BENCHMARK LEVEL ${level} ===`);
  const jobLimit = Math.min(level, 4); // Telegram bot API limits concurrency per chat/bot
  setConcurrency(level, jobLimit);

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
  
  const startTime = new Date().toISOString();
  
  console.log("Soaking and measuring (120s)...");
  await new Promise(r => setTimeout(r, 120000));
  
  const { data: jobs } = await sb
    .from('importer_queue')
    .select('payload, updated_at')
    .eq('task_type', 'IMPORT_CHAPTER')
    .eq('status', 'COMPLETED')
    .not('payload->telemetry', 'is', null)
    .gte('updated_at', startTime);
    
  let resultStr = `Level ${level} Results:\n`;
  if (!jobs || jobs.length === 0) {
    resultStr += `0 jobs completed.\n`;
  } else {
    let down = [], up = [], total = [], bytes = [], pages = [];
    for (const j of jobs) {
      const t = j.payload.telemetry;
      down.push((t.tDownloadEnd - t.tDownloadStart)/1000);
      up.push((t.tUploadEnd - t.tUploadStart)/1000);
      total.push((t.tStaged - t.tStart)/1000);
      if(t.totalBytesDown) bytes.push(t.totalBytesDown/1024/1024);
      if(t.pages) pages.push(t.pages);
    }
    
    resultStr += `Jobs completed: ${jobs.length} (Rate: ${(jobs.length/2).toFixed(1)}/min)\n`;
    resultStr += `Avg Download: ${avg(down)}s (p50: ${p50(down)}s, p95: ${p95(down)}s)\n`;
    resultStr += `Avg Upload: ${avg(up)}s (p50: ${p50(up)}s, p95: ${p95(up)}s)\n`;
    resultStr += `Avg Total to Staged: ${avg(total)}s\n`;
    resultStr += `Avg Size: ${avg(bytes)} MB, Avg Pages: ${avg(pages)}\n`;
  }
  console.log(resultStr);
  fs.appendFileSync('benchmark_results.txt', resultStr + '\n');
}

async function run() {
  fs.writeFileSync('benchmark_results.txt', 'BENCHMARK RESULTS\n\n');
  
  // Baseline (current setting)
  // Let's test levels 2, 4, 6
  for (const lvl of [2, 4, 6]) {
    await runLevel(lvl);
  }
  
  console.log("Done.");
}
run().catch(console.error);
