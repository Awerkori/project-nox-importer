import pg from 'pg';
import https from 'https';
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;
const DB_CONFIG = {
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
};

const HOME_URL = 'https://manga.project-nox-awerkori.workers.dev/';
const READER_URL = 'https://manga.project-nox-awerkori.workers.dev/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c';
const MEDIA_URL = 'https://manga.project-nox-awerkori.workers.dev/media/000003ed-c2db-4794-bcfa-c5e8b21ce080';

const agent = new https.Agent({ keepAlive: true, maxSockets: 10 });

function measureTTFB(url, timeoutMs = 4000) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = https.get(url, { agent }, (res) => {
      let resolved = false;
      res.once('data', () => {
        if (!resolved) {
          resolved = true;
          const ttfb = Math.round(performance.now() - t0);
          req.destroy();
          resolve({ status: res.statusCode, ttfb });
        }
      });
      res.on('end', () => {
        if (!resolved) {
          resolved = true;
          resolve({ status: res.statusCode, ttfb: Math.round(performance.now() - t0) });
        }
      });
    });
    req.on('error', (err) => resolve({ status: 500, ttfb: 9999, error: err.message }));
    req.setTimeout(timeoutMs, () => { req.destroy(); resolve({ status: 408, ttfb: 9999 }); });
  });
}

const percentile = (arr, p) => {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
};

const avg = (arr) => arr.length ? (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1) : '0';

async function main() {
  const durationMinutes = parseFloat(process.argv[2] || '20');
  const sampleIntervalSec = parseInt(process.argv[3] || '20', 10);
  const durationMs = durationMinutes * 60 * 1000;

  console.log(`Starting clean baseline measurement: ${durationMinutes} minutes, sample interval: ${sampleIntervalSec}s...`);

  const client = new Client(DB_CONFIG);
  await client.connect();

  // Warmup keepalive connection to site
  await measureTTFB(HOME_URL);
  await measureTTFB(READER_URL);
  await measureTTFB(MEDIA_URL);

  const startTime = Date.now();
  const samples = [];
  const homeLatencies = [];
  const readerLatencies = [];
  const mediaLatencies = [];

  let sampleIdx = 0;

  try {
    while (Date.now() - startTime < durationMs) {
      sampleIdx++;
      const t0 = Date.now();

      // 1. Database snapshot
      const actRes = await client.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
               count(*) FILTER (WHERE state = 'active') as active,
               count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
        FROM pg_stat_activity
      `);

      const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
      const m = ybRes.rows[0]?.metrics || {};
      const cpu = (parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100;

      // 2. Locks check
      const locksRes = await client.query(`
        SELECT count(*) as blocking
        FROM pg_locks
        WHERE NOT granted
      `);

      // 3. Queue check
      const queueRes = await client.query(`
        SELECT count(*) as active_chapters
        FROM importer_queue
        WHERE task_type = 'IMPORT_CHAPTER' AND status IN ('IMPORTING', 'RUNNING')
      `);

      // 4. Site latency probes
      const homeRes = await measureTTFB(HOME_URL);
      const readerRes = await measureTTFB(READER_URL);
      const mediaRes = await measureTTFB(MEDIA_URL);

      if (homeRes.ttfb < 9000) homeLatencies.push(homeRes.ttfb);
      if (readerRes.ttfb < 9000) readerLatencies.push(readerRes.ttfb);
      if (mediaRes.ttfb < 9000) mediaLatencies.push(mediaRes.ttfb);

      const sample = {
        idx: sampleIdx,
        timestamp: new Date().toISOString(),
        elapsedSec: Math.round((Date.now() - startTime) / 1000),
        totalConns: parseInt(actRes.rows[0].total, 10),
        hyperdriveConns: parseInt(actRes.rows[0].hyperdrive, 10),
        activeConns: parseInt(actRes.rows[0].active, 10),
        idleInTx: parseInt(actRes.rows[0].idle_in_tx, 10),
        blockingLocks: parseInt(locksRes.rows[0].blocking, 10),
        activeChapters: parseInt(queueRes.rows[0].active_chapters, 10),
        cpuPercent: parseFloat(cpu.toFixed(2)),
        homeTtfb: homeRes.ttfb,
        readerTtfb: readerRes.ttfb,
        mediaTtfb: mediaRes.ttfb
      };

      samples.push(sample);

      console.log(
        `[#${sampleIdx} ${sample.elapsedSec}s/${Math.round(durationMs/1000)}s] ` +
        `CPU: ${sample.cpuPercent.toFixed(1)}% | ` +
        `YSQL: ${sample.totalConns}/13 (Hyp: ${sample.hyperdriveConns}, Act: ${sample.activeConns}, IdleTx: ${sample.idleInTx}) | ` +
        `ActiveCh: ${sample.activeChapters} | ` +
        `Home: ${sample.homeTtfb}ms | Reader: ${sample.readerTtfb}ms | Media: ${sample.mediaTtfb}ms`
      );

      const elapsed = Date.now() - t0;
      const waitTime = Math.max(100, (sampleIntervalSec * 1000) - elapsed);
      await new Promise(r => setTimeout(r, waitTime));
    }
  } finally {
    await client.end();
  }

  // Summary statistics
  const cpus = samples.map(s => s.cpuPercent);
  const conns = samples.map(s => s.totalConns);
  const hyps = samples.map(s => s.hyperdriveConns);
  const activeJobs = samples.map(s => s.activeChapters);

  const summary = {
    durationMinutes,
    totalSamples: samples.length,
    ysql: {
      avg: avg(conns),
      min: Math.min(...conns),
      max: Math.max(...conns),
      p95: percentile(conns, 0.95),
      idleInTxMax: Math.max(...samples.map(s => s.idleInTx)),
      blockingLocksMax: Math.max(...samples.map(s => s.blockingLocks))
    },
    hyperdrive: {
      avg: avg(hyps),
      min: Math.min(...hyps),
      max: Math.max(...hyps),
      p95: percentile(hyps, 0.95)
    },
    cpu: {
      avg: avg(cpus),
      p50: percentile(cpus, 0.50),
      p95: percentile(cpus, 0.95),
      peak: Math.max(...cpus),
      above60Count: cpus.filter(c => c >= 60).length,
      above80Count: cpus.filter(c => c >= 80).length
    },
    site: {
      homeP50: percentile(homeLatencies, 0.50),
      homeP95: percentile(homeLatencies, 0.95),
      homePeak: Math.max(...homeLatencies),
      readerP50: percentile(readerLatencies, 0.50),
      readerP95: percentile(readerLatencies, 0.95),
      readerPeak: Math.max(...readerLatencies),
      mediaP50: percentile(mediaLatencies, 0.50),
      mediaP95: percentile(mediaLatencies, 0.95),
      mediaPeak: Math.max(...mediaLatencies)
    },
    activeChapters: {
      avg: avg(activeJobs),
      max: Math.max(...activeJobs)
    }
  };

  console.log('\n======================================================================');
  console.log('📊 20-MINUTE CLEAN BASELINE SUMMARY REPORT');
  console.log('======================================================================');
  console.log(JSON.stringify(summary, null, 2));

  fs.writeFileSync('clean_baseline_20min_result.json', JSON.stringify({ summary, samples }, null, 2));
  console.log('\nResults saved to clean_baseline_20min_result.json');
}

main().catch(console.error);
