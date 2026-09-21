import pg from 'pg';
import https from 'node:https';
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

const DISCLOUD_TOKEN = '4bc8293a4be7d5e98fe447b89d6d3bb0e12897b1b421820829d4ee8127348f2404af1f927eea5a62c186f98bc11ef7676a9f9a68';
const APP_ID = '1788873398156';

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
        if (!ttfbRecorded) ttfb = Math.round(performance.now() - t0);
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

function percentile(arr, p) {
  if (!arr || !arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
}

async function getDiscloudStatus() {
  try {
    const res = await fetch(`https://gw.discloud.com/api/app/${APP_ID}/status`, {
      headers: { Authorization: `Bearer ${DISCLOUD_TOKEN}` },
      signal: AbortSignal.timeout(4000)
    });
    if (res.ok) {
      const data = await res.json();
      const app = data.app || {};
      const cpu = parseFloat((app.cpu || '0').replace('%', ''));
      const ram = parseFloat(app.ram || 0);
      return { cpu, ram, memory: app.memory, container: app.container };
    }
  } catch (err) {}
  return { cpu: 0, ram: 0, memory: '', container: 'unknown' };
}

async function main() {
  const durationSec = parseInt(process.argv[2] || '180', 10);
  const sampleIntervalSec = parseInt(process.argv[3] || '10', 10);

  console.log('======================================================================');
  console.log('📡 PROJECT NOX IMPORTER — MONITORAMENTO INICIAL DE PRODUÇÃO');
  console.log(`   Duração da Observação: ${durationSec}s (${(durationSec / 60).toFixed(1)} min)`);
  console.log('   Modo: PRODUÇÃO REAL (Sem injeção artificial)');
  console.log('======================================================================\n');

  const client = new Client(DB_CONFIG);
  await client.connect();

  const startTimeRes = await client.query('SELECT NOW() as start_time');
  const startIso = startTimeRes.rows[0].start_time.toISOString();
  console.log(`[Monitor] Observando jobs a partir de: ${startIso}\n`);

  const startTime = Date.now();
  const durationMs = durationSec * 1000;

  const homeLatencies = [];
  const readerLatencies = [];
  const mediaLatencies = [];
  const ysqlConnSamples = [];
  const ybCpuSamples = [];
  const discloudCpuSamples = [];
  const discloudRamSamples = [];
  const activeWorkerSamples = [];
  let sampleIdx = 0;

  try {
    while (Date.now() - startTime < durationMs) {
      sampleIdx++;
      const t0 = Date.now();
      const elapsedSec = Math.round((Date.now() - startTime) / 1000);
      const elapsedMin = elapsedSec > 0 ? elapsedSec / 60 : 0.01;

      // 1. Database activity
      const actRes = await client.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
               count(*) FILTER (WHERE application_name = 'project-nox-importer-direct') as direct_importer,
               count(*) FILTER (WHERE state = 'active') as active,
               count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
        FROM pg_stat_activity
      `);
      const totalConns = parseInt(actRes.rows[0].total, 10);
      const directConns = parseInt(actRes.rows[0].direct_importer, 10);
      const hyperdriveConns = parseInt(actRes.rows[0].hyperdrive, 10);
      const idleInTx = parseInt(actRes.rows[0].idle_in_tx, 10);
      ysqlConnSamples.push(totalConns);

      // YB CPU
      const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
      const m = ybRes.rows[0]?.metrics || {};
      const cpu = (parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100;
      ybCpuSamples.push(cpu);

      // 2. Queue State
      const qRes = await client.query(`
        SELECT count(*) FILTER (WHERE status = 'IMPORTING') as importing,
               count(*) FILTER (WHERE status = 'QUEUED') as queued,
               count(*) FILTER (WHERE status = 'COMPLETED' AND updated_at >= $1) as completed_since_start,
               count(*) FILTER (WHERE status = 'FAILED' AND updated_at >= $1) as failed_since_start,
               count(*) FILTER (WHERE status = 'IMPORTING' AND updated_at < NOW() - INTERVAL '5 minutes') as stuck_count
        FROM importer_queue
      `, [startIso]);
      const importing = parseInt(qRes.rows[0].importing, 10);
      const queued = parseInt(qRes.rows[0].queued, 10);
      const completed = parseInt(qRes.rows[0].completed_since_start, 10);
      const failed = parseInt(qRes.rows[0].failed_since_start, 10);
      const stuck = parseInt(qRes.rows[0].stuck_count, 10);
      activeWorkerSamples.push(importing);

      // Media throughput
      const mediaRes = await client.query(`
        SELECT count(*) as count, COALESCE(sum(bytes), 0) as total_bytes
        FROM media
        WHERE created_at >= $1
      `, [startIso]);
      const mediaCount = parseInt(mediaRes.rows[0].count, 10);
      const mediaBytes = parseInt(mediaRes.rows[0].total_bytes, 10);
      const mediaMb = (mediaBytes / (1024 * 1024)).toFixed(1);

      const capPerMin = (completed / elapsedMin).toFixed(1);
      const mbPerMin = (parseFloat(mediaMb) / elapsedMin).toFixed(1);

      // 3. Site TTFB
      const homeRes = await measureTTFB(HOME_URL, homeAgent);
      const readerRes = await measureTTFB(READER_URL, readerAgent);
      const mediaProbeRes = await measureTTFB(MEDIA_URL, mediaAgent);
      if (homeRes.ttfb < 9000) homeLatencies.push(homeRes.ttfb);
      if (readerRes.ttfb < 9000) readerLatencies.push(readerRes.ttfb);
      if (mediaProbeRes.ttfb < 9000) mediaLatencies.push(mediaProbeRes.ttfb);

      // 4. Discloud container status
      const discloud = await getDiscloudStatus();
      if (discloud.cpu > 0) discloudCpuSamples.push(discloud.cpu);
      if (discloud.ram > 0) discloudRamSamples.push(discloud.ram);

      console.log(
        `[#${sampleIdx} ${elapsedSec}s/${durationSec}s] ` +
        `Workers: ${importing}/18 (${queued} queued) | ` +
        `Done: ${completed} cap (${capPerMin} c/m, ${mediaCount} pgs, ${mbPerMin} MB/m) | ` +
        `Fail: ${failed} | Stuck: ${stuck} | ` +
        `YSQL: ${totalConns}/13 (Dir: ${directConns}, Hyp: ${hyperdriveConns}) | ` +
        `YB CPU: ${cpu.toFixed(1)}% | ` +
        `Discloud: ${discloud.cpu}% CPU, ${discloud.ram.toFixed(0)}MB | ` +
        `Site TTFB: H:${homeRes.ttfb}ms R:${readerRes.ttfb}ms M:${mediaProbeRes.ttfb}ms`
      );

      // Tripwires
      if (totalConns >= 11) {
        console.warn(`⚠️ TRIPWIRE ALERT: YSQL connections touched ${totalConns}/13`);
      }
      if (totalConns >= 12) {
        console.error(`🚨 TRIPWIRE CRITICAL: YSQL connections reached ${totalConns}/13`);
        await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");
        break;
      }

      const elapsed = Date.now() - t0;
      const waitTime = Math.max(100, (sampleIntervalSec * 1000) - elapsed);
      await new Promise(r => setTimeout(r, waitTime));
    }
  } finally {
    // Collect active sources distribution
    const sourceRes = await client.query(`
      SELECT source, count(*) as count
      FROM importer_queue
      WHERE status = 'COMPLETED' AND updated_at >= $1
      GROUP BY source
      ORDER BY count DESC
    `, [startIso]);

    console.log('\n======================================================================');
    console.log('📊 RESUMO DO MONITORAMENTO INICIAL DE PRODUÇÃO');
    console.log('======================================================================');
    console.log(`Active Sources com conclusão no período:`);
    console.table(sourceRes.rows);

    console.log(`Throughput: ${activeWorkerSamples.slice(-1)[0]} workers ativos | CAP/MIN final: ${(sourceRes.rows.reduce((acc, r) => acc + parseInt(r.count, 10), 0) / (durationSec / 60)).toFixed(2)}`);
    console.log(`YSQL Connections: avg ${(ysqlConnSamples.reduce((a,b)=>a+b,0)/ysqlConnSamples.length).toFixed(1)} | peak ${Math.max(...ysqlConnSamples)}/13`);
    console.log(`Site TTFB: Home p95=${percentile(homeLatencies, 0.95)}ms | Reader p95=${percentile(readerLatencies, 0.95)}ms | Media p95=${percentile(mediaLatencies, 0.95)}ms`);
    console.log(`Discloud: CPU avg ${(discloudCpuSamples.reduce((a,b)=>a+b,0)/discloudCpuSamples.length).toFixed(1)}% | RAM avg ${(discloudRamSamples.reduce((a,b)=>a+b,0)/discloudRamSamples.length).toFixed(0)}MB`);
    console.log('======================================================================\n');

    await client.end();
  }
}

main().catch(console.error);
