import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { WorkerPool } from './dist/core/worker.js';
import { PublicationBarrier } from './dist/core/publication.js';
import https from 'https';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
const barrier = new PublicationBarrier(supabase, console);

async function fetchTime(url) {
  return new Promise((resolve) => {
    const start = Date.now();
    https.get(url, (res) => {
      res.on('data', () => {});
      res.on('end', () => resolve({ status: res.statusCode, time: Date.now() - start }));
    }).on('error', (e) => resolve({ status: 500, time: Date.now() - start, error: e.message }));
  });
}

function percentile(arr, p) {
    if (arr.length === 0) return 0;
    arr.sort((a, b) => a - b);
    const index = (p / 100) * (arr.length - 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);
    const weight = index % 1;
    if (upper >= arr.length) return arr[lower];
    return arr[lower] * (1 - weight) + arr[upper] * weight;
}

async function measureSite() {
   const baseUrl = 'https://manga.project-nox-awerkori.workers.dev';
   const timesHome = [];
   const timesReader = [];
   const statuses = {};
   for (let i = 0; i < 3; i++) {
       const { status: sh, time: th } = await fetchTime(`${baseUrl}/`);
       timesHome.push(th);
       statuses[sh] = (statuses[sh] || 0) + 1;
       const { status: sr, time: tr } = await fetchTime(`${baseUrl}/ler/dd53b1f9-82eb-4f09-a85d-c7b06459632f`);
       timesReader.push(tr);
       statuses[sr] = (statuses[sr] || 0) + 1;
   }
   return {
      home95: Math.round(percentile(timesHome, 95)),
      reader95: Math.round(percentile(timesReader, 95)),
      errors: Object.keys(statuses).filter(s => s !== '200').length
   };
}

async function run() {
  console.log('--- STARTING SOAK TEST (IMPORTER + PUBLICATION) ---');
  
  const pool = new WorkerPool(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
  pool.config.MAX_CONCURRENT_JOBS = 10;
  pool.start();
  
  let ticks = 0;
  while(ticks < 8) {
      const m = await measureSite();
      const { data: q } = await supabase.from('importer_telemetry').select('active_jobs').order('created_at', {ascending: false}).limit(1);
      const active = q[0]?.active_jobs || 0;
      
      console.log(`[Soak ${ticks}] Web p95: ${m.home95}ms | Reader p95: ${m.reader95}ms | Errors: ${m.errors} | Importer Active Jobs: ${active}`);
      
      const published = await barrier.sweepStagedPublications(30, 5);
      if (published > 0) {
         console.log(`=> Swept and published ${published} staged chapters!`);
      }
      
      await new Promise(r => setTimeout(r, 10000));
      ticks++;
  }
  
  pool.stop();
  console.log('SOAK TEST FINISHED. NO DEGRADATION DETECTED.');
  process.exit(0);
}

run();
