import pg from 'pg';
import https from 'https';
import dotenv from 'dotenv';
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

const homeAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
const readerAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
const mediaAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });

function measureTTFB(url, agent, timeoutMs = 4000) {
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
        resolve({ status: res.statusCode, ttfb });
      });
    });

    req.on('error', (err) => resolve({ status: 500, ttfb: 9999, error: err.message }));
    req.setTimeout(timeoutMs, () => {
      req.destroy();
      resolve({ status: 408, ttfb: 9999 });
    });
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
  console.log('======================================================================');
  console.log('📊 COLLECTING 2-MINUTE CLEAN BASELINE (DISCOVERY PAUSED, 0 ACTIVE JOBS)');
  console.log('======================================================================\n');

  const client = new Client(DB_CONFIG);
  await client.connect();

  // Warmup keepalive
  await measureTTFB(HOME_URL, homeAgent);
  await measureTTFB(READER_URL, readerAgent);
  await measureTTFB(MEDIA_URL, mediaAgent);

  const durationSec = 120;
  const intervalSec = 5;
  const totalSamples = durationSec / intervalSec;

  const ysqlConns = [];
  const hyperdriveConns = [];
  const directConns = [];
  const ybCpus = [];
  const homeTtfbs = [];
  const readerTtfbs = [];
  const mediaTtfbs = [];
  let idleInTxViolations = 0;
  let blockingLockViolations = 0;

  for (let i = 1; i <= totalSamples; i++) {
    const t0 = Date.now();

    const actRes = await client.query(`
      SELECT count(*) as total,
             count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
             count(*) FILTER (WHERE application_name = 'project-nox-importer-direct') as direct_importer,
             count(*) FILTER (WHERE state = 'active') as active,
             count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
      FROM pg_stat_activity
    `);
    const act = actRes.rows[0];

    const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
    const m = ybRes.rows[0]?.metrics || {};
    const cpu = (parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100;

    const locksRes = await client.query(`
      SELECT count(*) as blocking
      FROM pg_locks
      WHERE NOT granted
    `);
    const blocking = parseInt(locksRes.rows[0].blocking, 10);

    const home = await measureTTFB(HOME_URL, homeAgent);
    const reader = await measureTTFB(READER_URL, readerAgent);
    const media = await measureTTFB(MEDIA_URL, mediaAgent);

    const totalC = parseInt(act.total, 10);
    const hypC = parseInt(act.hyperdrive, 10);
    const dirC = parseInt(act.direct_importer, 10);
    const idleTx = parseInt(act.idle_in_tx, 10);

    if (idleTx > 0) idleInTxViolations++;
    if (blocking > 0) blockingLockViolations++;

    ysqlConns.push(totalC);
    hyperdriveConns.push(hypC);
    directConns.push(dirC);
    ybCpus.push(cpu);
    if (home.ttfb < 9000) homeTtfbs.push(home.ttfb);
    if (reader.ttfb < 9000) readerTtfbs.push(reader.ttfb);
    if (media.ttfb < 9000) mediaTtfbs.push(media.ttfb);

    console.log(
      `[#${i}/${totalSamples} ${i*intervalSec}s] ` +
      `YSQL: ${totalC}/13 (Hyp: ${hypC}, Direct: ${dirC}) | ` +
      `YB CPU: ${cpu.toFixed(1)}% | ` +
      `Site: H:${home.ttfb}ms R:${reader.ttfb}ms M:${media.ttfb}ms | ` +
      `idle_in_tx: ${idleTx}`
    );

    const elapsed = Date.now() - t0;
    const wait = Math.max(50, (intervalSec * 1000) - elapsed);
    await new Promise(r => setTimeout(r, wait));
  }

  await client.end();

  const result = {
    durationMinutes: 2,
    totalSamples: ysqlConns.length,
    ysql: {
      avg: avg(ysqlConns),
      p95: percentile(ysqlConns, 0.95),
      peak: Math.max(...ysqlConns)
    },
    hyperdrive: {
      avg: avg(hyperdriveConns),
      p95: percentile(hyperdriveConns, 0.95),
      peak: Math.max(...hyperdriveConns)
    },
    directImporter: {
      avg: avg(directConns),
      p95: percentile(directConns, 0.95),
      peak: Math.max(...directConns)
    },
    ybCpu: {
      avg: avg(ybCpus),
      p95: percentile(ybCpus, 0.95).toFixed(1),
      peak: Math.max(...ybCpus).toFixed(1)
    },
    site: {
      home_p95: percentile(homeTtfbs, 0.95),
      reader_p95: percentile(readerTtfbs, 0.95),
      media_p95: percentile(mediaTtfbs, 0.95)
    },
    idleInTxViolations,
    blockingLockViolations
  };

  console.log('\n======================================================================');
  console.log('📋 2-MINUTE CLEAN BASELINE SUMMARY');
  console.log('======================================================================');
  console.log(`YSQL CONNECTIONS: avg ${result.ysql.avg}/13, p95 ${result.ysql.p95}/13, peak ${result.ysql.peak}/13`);
  console.log(`HYPERDRIVE CONNECTIONS: avg ${result.hyperdrive.avg}, p95 ${result.hyperdrive.p95}, peak ${result.hyperdrive.peak}`);
  console.log(`DIRECT IMPORTER CONNECTIONS: avg ${result.directImporter.avg}, p95 ${result.directImporter.p95}, peak ${result.directImporter.peak}`);
  console.log(`YUGABYTE CPU: avg ${result.ybCpu.avg}%, p95 ${result.ybCpu.p95}%, peak ${result.ybCpu.peak}%`);
  console.log(`HOME p95: ${result.site.home_p95}ms (target <= 250ms)`);
  console.log(`READER p95: ${result.site.reader_p95}ms (target <= 150ms)`);
  console.log(`MEDIA p95: ${result.site.media_p95}ms (target <= 120ms)`);
  console.log(`IDLE IN TRANSACTION VIOLATIONS: ${result.idleInTxViolations}`);
  console.log(`BLOCKING LOCK VIOLATIONS: ${result.blockingLockViolations}`);
  console.log('======================================================================\n');

  import('fs').then(fs => {
    fs.writeFileSync('clean_baseline_2min_direct.json', JSON.stringify(result, null, 2));
  });
}

main().catch(console.error);
