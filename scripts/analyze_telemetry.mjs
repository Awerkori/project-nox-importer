import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

async function run() {
  const { data: jobs } = await sb
    .from('importer_queue')
    .select('id, status, payload, updated_at')
    .eq('task_type', 'IMPORT_CHAPTER')
    .eq('status', 'COMPLETED')
    .not('payload->telemetry', 'is', null)
    .order('updated_at', { ascending: false })
    .limit(50);
    
  if (!jobs || jobs.length === 0) {
    console.log("No telemetry jobs found yet.");
    return;
  }
  
  console.log(`Found ${jobs.length} jobs with telemetry.`);
  
  let tDownloadMs = [];
  let tUploadMs = [];
  let tValidateMs = [];
  let bytes = [];
  let pages = [];
  let tTotalStagedMs = [];
  
  for (const job of jobs) {
    const t = job.payload.telemetry;
    const download = t.tDownloadEnd - t.tDownloadStart;
    const upload = t.tUploadEnd - t.tUploadStart;
    const validate = t.tDownloadStart - t.tStart;
    const totalToStaged = t.tStaged - t.tStart;
    
    tDownloadMs.push(download);
    tUploadMs.push(upload);
    tValidateMs.push(validate);
    tTotalStagedMs.push(totalToStaged);
    if (t.totalBytesDown) bytes.push(t.totalBytesDown);
    if (t.pages) pages.push(t.pages);
    
    console.log(`[${t.source} - ${t.workId} ch${t.chapter}] Pages: ${t.pages}, Size: ${(t.totalBytesDown/1024/1024).toFixed(2)}MB, Down: ${download/1000}s, Up: ${upload/1000}s, Total(Staged): ${totalToStaged/1000}s`);
  }
  
  const avg = (arr) => arr.reduce((a, b) => a + b, 0) / arr.length;
  const p50 = (arr) => arr.sort((a,b)=>a-b)[Math.floor(arr.length*0.5)];
  const p95 = (arr) => arr.sort((a,b)=>a-b)[Math.floor(arr.length*0.95)];
  
  console.log("\n--- AGGREGATES ---");
  console.log(`Avg Chapter Size: ${(avg(bytes)/1024/1024).toFixed(2)} MB`);
  console.log(`Avg Pages: ${avg(pages).toFixed(1)}`);
  console.log(`Avg Validate: ${avg(tValidateMs).toFixed(0)} ms`);
  console.log(`Download (p50): ${(p50(tDownloadMs)/1000).toFixed(2)} s`);
  console.log(`Download (p95): ${(p95(tDownloadMs)/1000).toFixed(2)} s`);
  console.log(`Upload (p50): ${(p50(tUploadMs)/1000).toFixed(2)} s`);
  console.log(`Upload (p95): ${(p95(tUploadMs)/1000).toFixed(2)} s`);
  console.log(`Total Staged (p50): ${(p50(tTotalStagedMs)/1000).toFixed(2)} s`);
}
run().catch(console.error);
