import { spawn } from 'child_process';
import https from 'https';

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

async function run() {
  console.log('--- STARTING 30-MINUTE SOAK TEST (IMPORTER + PUBLICATION) ---');
  
  const startTime = Date.now();
  const soakDurationMs = 30 * 60 * 1000;
  
  const importer = spawn('node', ['build/index.js'], {
      env: { ...process.env, TESTED_CONCURRENCY_CEILING: '6' },
      stdio: 'inherit'
  });
  
  const metricsArr = [];
  
  while(Date.now() - startTime < soakDurationMs) {
      const baseUrl = 'https://manga.project-nox-awerkori.workers.dev';
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
          const { status: sc } = await fetchTime(`${baseUrl}/catalogo`);
          statuses[sc] = (statuses[sc] || 0) + 1;
          
          await new Promise(r => setTimeout(r, 2000));
      }
      
      const home95 = Math.round(percentile(timesHome, 95));
      const reader95 = Math.round(percentile(timesReader, 95));
      const errors = Object.keys(statuses).filter(s => s !== '200' && s !== '304').length;
      
      console.log(`[Soak] Web p95: ${home95}ms | Reader p95: ${reader95}ms | Errors: ${errors}`);
      metricsArr.push({ home95, reader95, errors });
      
      await new Promise(r => setTimeout(r, 45000));
  }
  
  importer.kill();
  
  const avgHome = Math.round(metricsArr.reduce((s, m) => s + m.home95, 0) / metricsArr.length);
  const avgReader = Math.round(metricsArr.reduce((s, m) => s + m.reader95, 0) / metricsArr.length);
  const totalErrors = metricsArr.reduce((s, m) => s + m.errors, 0);
  
  console.log('--- SOAK TEST FINISHED ---');
  console.log(`Avg Web p95: ${avgHome}ms`);
  console.log(`Avg Reader p95: ${avgReader}ms`);
  console.log(`Total Errors: ${totalErrors}`);
  process.exit(0);
}

run();
