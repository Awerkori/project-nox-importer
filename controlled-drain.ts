import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { PublicationBarrier } from './src/core/publication';
import https from 'https';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env: any = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const barrier = new PublicationBarrier(supabase, console as any);
const baseUrl = 'https://manga.project-nox-awerkori.workers.dev';

async function fetchTime(url) {
  return new Promise((resolve) => {
    const start = Date.now();
    https.get(url, (res) => {
      let data = '';
      res.on('data', (c) => { data += c; });
      res.on('end', () => {
        resolve({ status: res.statusCode, time: Date.now() - start });
      });
    }).on('error', (e) => {
      resolve({ status: 500, time: Date.now() - start, error: e.message });
    });
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

async function measureP95() {
   const timesHome = [];
   const timesReader = [];
   const statuses = {};
   
   for (let i = 0; i < 5; i++) {
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
  console.log('--- STARTING CONTROLLED PUBLICATION DRAIN ---');
  
  let capPerMinute = 10;
  let burst = 2;
  let publishedTotalSession = 0;
  
  while (true) {
      console.log(`\nPublishing up to ${capPerMinute} chapters...`);
      const published = await barrier.sweepStagedPublications(capPerMinute, burst);
      publishedTotalSession += published;
      
      console.log(`Published ${published} chapters in this tick. (Session total: ${publishedTotalSession})`);
      
      if (published === 0) {
          console.log('No more staged chapters to publish or blocked by barriers. Drain complete!');
          break;
      }
      
      console.log('Measuring site health under load...');
      const metrics = await measureP95();
      console.log(`Home p95: ${metrics.home95}ms | Reader p95: ${metrics.reader95}ms | Errors: ${metrics.errors}`);
      
      if (metrics.errors > 0 || metrics.home95 > 800 || metrics.reader95 > 800) {
          console.log('DEGRADATION DETECTED! Scaling down...');
          capPerMinute = Math.max(5, Math.floor(capPerMinute / 2));
          burst = Math.max(1, burst - 1);
          await new Promise(r => setTimeout(r, 10000)); // wait for recovery
      } else {
          if (metrics.home95 < 300 && metrics.reader95 < 300) {
              capPerMinute = Math.min(100, capPerMinute + 10);
              burst = Math.min(6, burst + 1);
              console.log('Site is stable! Scaling up cap to', capPerMinute);
          }
      }
      
      // Wait for the rest of the minute to simulate a per-minute rate, or just wait 10 seconds for faster testing.
      await new Promise(r => setTimeout(r, 10000));
  }
}

run();
