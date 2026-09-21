import https from 'node:https';

const HOME_URL = 'https://manga.project-nox-awerkori.workers.dev/';
const READER_URL = 'https://manga.project-nox-awerkori.workers.dev/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c';
const MEDIA_URL = 'https://manga.project-nox-awerkori.workers.dev/media/000003ed-c2db-4794-bcfa-c5e8b21ce080';

const homeAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
const readerAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
const mediaAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });

function measureTTFB(url, agent, timeoutMs = 5000) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    let ttfbRecorded = false;
    let ttfb = 0;

    const req = https.get(url, { agent }, (res) => {
      res.once('data', () => {
        if (!ttfbRecorded) {
          ttfbRecorded = true;
          ttfb = Math.round(performance.now() - t0);
        }
      });

      res.resume();
      res.on('end', () => {
        if (!ttfbRecorded) {
          ttfb = Math.round(performance.now() - t0);
        }
        resolve({ status: res.statusCode, ttfb, error: null });
      });
    });

    req.on('error', (err) => resolve({ status: 500, ttfb: 9999, error: err.message }));
    req.setTimeout(timeoutMs, () => {
      req.destroy();
      resolve({ status: 408, ttfb: 9999, error: 'TIMEOUT' });
    });
  });
}

function stats(arr) {
  if (!arr.length) return { p50: 0, p90: 0, p95: 0, p99: 0, max: 0, avg: '0' };
  const sorted = [...arr].sort((a, b) => a - b);
  const p = (pct) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * pct))];
  const avg = (sorted.reduce((a, b) => a + b, 0) / sorted.length).toFixed(1);
  return {
    p50: p(0.50),
    p90: p(0.90),
    p95: p(0.95),
    p99: p(0.99),
    max: Math.max(...sorted),
    avg,
  };
}

async function main() {
  console.log('--- WARMING UP CONNECTIONS (10 requests) ---');
  for (let i = 0; i < 3; i++) {
    await measureTTFB(HOME_URL, homeAgent);
    await measureTTFB(READER_URL, readerAgent);
    await measureTTFB(MEDIA_URL, mediaAgent);
    await new Promise(r => setTimeout(r, 100));
  }

  console.log('--- RUNNING CONTROLLED IDLE PROBES (40 samples per endpoint) ---');
  const homeSamples = [];
  const readerSamples = [];
  const mediaSamples = [];
  let errorCount = 0;
  let totalRequests = 0;

  for (let i = 0; i < 40; i++) {
    const h = await measureTTFB(HOME_URL, homeAgent);
    totalRequests++;
    if (h.status >= 400 || h.error) errorCount++;
    else homeSamples.push(h.ttfb);

    const r = await measureTTFB(READER_URL, readerAgent);
    totalRequests++;
    if (r.status >= 400 || r.error) errorCount++;
    else readerSamples.push(r.ttfb);

    const m = await measureTTFB(MEDIA_URL, mediaAgent);
    totalRequests++;
    if (m.status >= 400 || m.error) errorCount++;
    else mediaSamples.push(m.ttfb);

    await new Promise(res => setTimeout(res, 150));
  }

  const hStats = stats(homeSamples);
  const rStats = stats(readerSamples);
  const mStats = stats(mediaSamples);
  const errRate = ((errorCount / totalRequests) * 100).toFixed(2);

  console.log('\n========================================');
  console.log('IDLE SITE PROBE RESULTS:');
  console.log('========================================');
  console.log('HOME:   p50=' + hStats.p50 + 'ms, p90=' + hStats.p90 + 'ms, p95=' + hStats.p95 + 'ms, p99=' + hStats.p99 + 'ms, max=' + hStats.max + 'ms, avg=' + hStats.avg + 'ms');
  console.log('READER: p50=' + rStats.p50 + 'ms, p90=' + rStats.p90 + 'ms, p95=' + rStats.p95 + 'ms, p99=' + rStats.p99 + 'ms, max=' + rStats.max + 'ms, avg=' + rStats.avg + 'ms');
  console.log('MEDIA:  p50=' + mStats.p50 + 'ms, p90=' + mStats.p90 + 'ms, p95=' + mStats.p95 + 'ms, p99=' + mStats.p99 + 'ms, max=' + mStats.max + 'ms, avg=' + mStats.avg + 'ms');
  console.log('HTTP ERROR RATE: ' + errRate + '% (' + errorCount + '/' + totalRequests + ')');
  console.log('========================================');

  const targetsMet = hStats.p95 <= 250 && rStats.p95 <= 150 && mStats.p95 <= 120 && errRate === '0.00';
  console.log('ALL SITE TARGETS MET: ' + (targetsMet ? 'YES' : 'NO'));
}

main().catch(console.error);
