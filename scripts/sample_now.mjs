import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;
const client = new Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

const HOME_URL = 'https://manga.project-nox-awerkori.workers.dev/';
const READER_URL = 'https://manga.project-nox-awerkori.workers.dev/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c';
const MEDIA_URL = 'https://manga.project-nox-awerkori.workers.dev/media/000003ed-c2db-4794-bcfa-c5e8b21ce080';

async function probeLatency(url, timeoutMs = 4000) {
  const t0 = performance.now();
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
    await res.arrayBuffer();
    return Math.round(performance.now() - t0);
  } catch {
    return 9999;
  }
}

const percentile = (arr, p) => {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
};

const avg = (arr) => arr.length ? (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1) : '0';

async function main() {
  await client.connect();

  const maxConnRes = await client.query('SHOW max_connections');
  const maxConn = parseInt(maxConnRes.rows[0].max_connections, 10);

  console.log('Sampling Yugabyte connections and CPU over 30s (6 samples, 5s interval)...');
  const samples = [];
  const cpuSamples = [];

  for (let i = 0; i < 6; i++) {
    const actRes = await client.query(`
      SELECT count(*) as total,
             count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
             count(*) FILTER (WHERE state = 'idle') as idle,
             count(*) FILTER (WHERE state = 'active') as active,
             count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
      FROM pg_stat_activity
    `);
    const total = parseInt(actRes.rows[0].total, 10);
    const hyp = parseInt(actRes.rows[0].hyperdrive, 10);
    const idle = parseInt(actRes.rows[0].idle, 10);
    const act = parseInt(actRes.rows[0].active, 10);
    const idleInTx = parseInt(actRes.rows[0].idle_in_tx, 10);

    let cpu = 0;
    try {
      const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
      const m = ybRes.rows[0]?.metrics || {};
      cpu = (parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100;
    } catch {}

    samples.push({ total, hyp, idle, act, idleInTx });
    cpuSamples.push(cpu);

    if (i < 5) await new Promise(r => setTimeout(r, 5000));
  }

  // Detailed activity snapshot
  const currentActivity = await client.query(`
    SELECT pid, datname, usename, application_name, client_addr, backend_type, state, query
    FROM pg_stat_activity
    ORDER BY backend_start ASC
  `);

  // Settings
  const barrierRes = await client.query("SELECT value FROM settings WHERE key = 'publication_safety_barrier'");
  const barrier = barrierRes.rows[0]?.value || 'UNKNOWN';

  // Queue summary
  const queueRes = await client.query(`
    SELECT status, count(*) as count
    FROM importer_queue
    GROUP BY status
  `);

  // Active jobs
  const activeJobsRes = await client.query(`
    SELECT id, task_type, source, status, locked_by, locked_at, lease_expires_at, progress_current, progress_total, updated_at
    FROM importer_queue
    WHERE status = 'IMPORTING'
  `);

  // Completed in last 5m and 10m
  const comp5m = await client.query(`
    SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
    FROM importer_queue
    WHERE status = 'COMPLETED' 
      AND task_type = 'IMPORT_CHAPTER'
      AND updated_at >= NOW() - INTERVAL '5 minutes'
  `);
  const comp10m = await client.query(`
    SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
    FROM importer_queue
    WHERE status = 'COMPLETED' 
      AND task_type = 'IMPORT_CHAPTER'
      AND updated_at >= NOW() - INTERVAL '10 minutes'
  `);

  // Recent media in last 5m and 10m
  const med5m = await client.query(`
    SELECT count(*) as count, COALESCE(sum(bytes), 0) as bytes
    FROM media
    WHERE created_at >= NOW() - INTERVAL '5 minutes'
  `);
  const med10m = await client.query(`
    SELECT count(*) as count, COALESCE(sum(bytes), 0) as bytes
    FROM media
    WHERE created_at >= NOW() - INTERVAL '10 minutes'
  `);

  // Telemetry from discloud
  const telemRecent = await client.query(`
    SELECT *
    FROM importer_telemetry
    WHERE worker_id = 'discloud-importer-1'
    ORDER BY created_at DESC
    LIMIT 10
  `);

  await client.end();

  // Probing Site Latencies (10 probes each)
  console.log('Probing site latencies (10 probes each)...');
  const homeProbes = [];
  const readerProbes = [];
  const mediaProbes = [];
  for (let i = 0; i < 10; i++) {
    homeProbes.push(await probeLatency(HOME_URL));
    readerProbes.push(await probeLatency(READER_URL));
    mediaProbes.push(await probeLatency(MEDIA_URL));
    await new Promise(r => setTimeout(r, 150));
  }

  const totals = samples.map(s => s.total);
  const hyps = samples.map(s => s.hyp);
  const idles = samples.map(s => s.idle);
  const acts = samples.map(s => s.act);
  const idleInTxs = samples.map(s => s.idleInTx);

  console.log('\n--- SAMPLING RESULTS ---');
  console.log('Totals:', totals);
  console.log('Hyperdrive:', hyps);
  console.log('CPUs:', cpuSamples.map(c => c.toFixed(1) + '%'));

  console.log(JSON.stringify({
    maxConn,
    latestSample: samples[samples.length - 1],
    connMin: Math.min(...totals),
    connMax: Math.max(...totals),
    connAvg: avg(totals),
    hypMin: Math.min(...hyps),
    hypMax: Math.max(...hyps),
    maxIdleInTx: Math.max(...idleInTxs),
    cpuCurrent: cpuSamples[cpuSamples.length - 1].toFixed(1),
    cpuAvg: avg(cpuSamples),
    cpuPeak: Math.max(...cpuSamples).toFixed(1),
    cpuAbove70Count: cpuSamples.filter(c => c >= 70).length,
    connsGte8Count: totals.filter(c => c >= 8).length,
    connsGte10Count: totals.filter(c => c >= 10).length,
    connsGte11Count: totals.filter(c => c >= 11).length,
    connsGte12Count: totals.filter(c => c >= 12).length,
    activityRows: currentActivity.rows,
    barrier,
    queueSummary: queueRes.rows,
    activeJobs: activeJobsRes.rows,
    comp5m: comp5m.rows[0],
    comp10m: comp10m.rows[0],
    med5m: med5m.rows[0],
    med10m: med10m.rows[0],
    latestTelemetry: telemRecent.rows[0],
    homeP50: percentile(homeProbes.filter(x => x < 9000), 0.5),
    homeP95: percentile(homeProbes.filter(x => x < 9000), 0.95),
    readerP50: percentile(readerProbes.filter(x => x < 9000), 0.5),
    readerP95: percentile(readerProbes.filter(x => x < 9000), 0.95),
    mediaP50: percentile(mediaProbes.filter(x => x < 9000), 0.5),
    mediaP95: percentile(mediaProbes.filter(x => x < 9000), 0.95),
    homeProbes,
    readerProbes,
    mediaProbes
  }, null, 2));
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
