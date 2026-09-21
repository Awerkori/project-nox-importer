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

async function main() {
  await client.connect();

  // 1. SHOW max_connections
  const maxConnRes = await client.query('SHOW max_connections');
  const maxConn = parseInt(maxConnRes.rows[0].max_connections, 10);

  // 2. pg_stat_activity
  const actRes = await client.query(`
    SELECT pid, datname, usename, application_name, client_addr, backend_type, state, wait_event_type, wait_event, query
    FROM pg_stat_activity
  `);
  const totalConns = actRes.rows.length;
  const hyperdriveConns = actRes.rows.filter(r => r.application_name === 'Cloudflare Hyperdrive');
  const activeConns = actRes.rows.filter(r => r.state === 'active');
  const idleConns = actRes.rows.filter(r => r.state === 'idle');
  const idleInTxConns = actRes.rows.filter(r => r.state === 'idle in transaction');
  const systemConns = actRes.rows.filter(r => !r.client_addr);

  // 3. Yugabyte CPU metrics
  let currentCpu = 'N/A';
  try {
    const ybMetricsRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
    const m = ybMetricsRes.rows[0]?.metrics || {};
    currentCpu = ((parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100).toFixed(1);
  } catch (e) {
    currentCpu = `Error: ${e.message}`;
  }

  // 4. Settings
  const barrierRes = await client.query("SELECT value FROM settings WHERE key = 'publication_safety_barrier'");
  const barrier = barrierRes.rows[0]?.value || 'UNKNOWN';

  // 5. Active jobs in importer_queue
  const activeJobsRes = await client.query(`
    SELECT id, source, status, locked_by, locked_at, lease_expires_at, progress_current, progress_total, updated_at
    FROM importer_queue
    WHERE status = 'IMPORTING'
  `);

  // 6. Queue breakdown
  const queueSummaryRes = await client.query(`
    SELECT status, count(*) as count
    FROM importer_queue
    GROUP BY status
  `);

  // 7. Recent telemetry (last 5 min, last 10 min)
  const telem5m = await client.query(`
    SELECT *
    FROM importer_telemetry
    WHERE created_at >= NOW() - INTERVAL '5 minutes'
    ORDER BY created_at DESC
  `);
  const telem10m = await client.query(`
    SELECT *
    FROM importer_telemetry
    WHERE created_at >= NOW() - INTERVAL '10 minutes'
    ORDER BY created_at DESC
  `);

  // 8. Completed in last 5m and 10m
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

  // 9. Ungranted locks
  const locksRes = await client.query('SELECT count(*) FROM pg_locks WHERE NOT granted');

  await client.end();

  // 10. Live site probes (5 probes each)
  const homeProbes = [];
  const readerProbes = [];
  const mediaProbes = [];
  for (let i = 0; i < 5; i++) {
    homeProbes.push(await probeLatency(HOME_URL));
    readerProbes.push(await probeLatency(READER_URL));
    mediaProbes.push(await probeLatency(MEDIA_URL));
    await new Promise(r => setTimeout(r, 200));
  }

  console.log('=== YUGABYTE CONNECTION METRICS ===');
  console.log(`CURRENT CONNECTIONS: ${totalConns}`);
  console.log(`MAX CONNECTIONS: ${maxConn}`);
  console.log(`USAGE %: ${((totalConns / maxConn) * 100).toFixed(2)}%`);
  console.log(`HYPERDRIVE CONNECTIONS: ${hyperdriveConns.length}`);
  console.log(`ACTIVE: ${activeConns.length}`);
  console.log(`IDLE: ${idleConns.length}`);
  console.log(`IDLE IN TRANSACTION: ${idleInTxConns.length}`);
  console.log(`UNGRANTED LOCKS: ${locksRes.rows[0].count}`);
  console.log('\n--- CONNECTION DETAILS ---');
  actRes.rows.forEach(r => {
    console.log(`PID: ${r.pid} | App: ${r.application_name || 'internal'} | State: ${r.state || 'backend'} | Client: ${r.client_addr || 'local'} | Query: ${(r.query || '').slice(0, 60)}`);
  });

  console.log('\n=== CPU METRICS ===');
  console.log(`YUGABYTE CPU CURRENT: ${currentCpu}%`);

  console.log('\n=== QUEUE & BARRIER ===');
  console.log(`PUBLICATION BARRIER: ${barrier}`);
  console.log(`ACTIVE JOBS (IMPORTING): ${activeJobsRes.rows.length}`);
  activeJobsRes.rows.forEach(j => {
    console.log(`  Job ${j.id} | Source: ${j.source} | Locked by: ${j.locked_by} | Progress: ${j.progress_current}/${j.progress_total} | Updated: ${j.updated_at}`);
  });
  console.log('QUEUE SUMMARY:');
  queueSummaryRes.rows.forEach(q => console.log(`  ${q.status}: ${q.count}`));

  console.log('\n=== IMPORTER TELEMETRY ===');
  console.log(`Telemetry rows in last 5m: ${telem5m.rows.length}`);
  console.log(`Telemetry rows in last 10m: ${telem10m.rows.length}`);
  if (telem10m.rows.length > 0) {
    const latest = telem10m.rows[0];
    console.log(`Latest Telemetry (${latest.created_at}): RSS ${latest.rss_mb} MB | Heap ${latest.heap_used_mb}/${latest.heap_total_mb} MB | Concurrency: ${latest.concurrency} | Active: ${latest.active_jobs} | Lag: ${latest.event_loop_lag_ms} ms`);
  }

  console.log('\n=== THROUGHPUT ===');
  console.log(`Last 5 min: ${comp5m.rows[0].count} chapters, ${comp5m.rows[0].pages} pages -> Pages/min: ${(parseInt(comp5m.rows[0].pages, 10) / 5).toFixed(1)}, Cap/min: ${(parseInt(comp5m.rows[0].count, 10) / 5).toFixed(2)}`);
  console.log(`Last 10 min: ${comp10m.rows[0].count} chapters, ${comp10m.rows[0].pages} pages -> Pages/min: ${(parseInt(comp10m.rows[0].pages, 10) / 10).toFixed(1)}, Cap/min: ${(parseInt(comp10m.rows[0].count, 10) / 10).toFixed(2)}`);

  console.log('\n=== SITE LATENCIES (5 PROBES) ===');
  console.log(`HOME: probes = [${homeProbes.join(', ')}] -> p50: ${percentile(homeProbes, 0.5)}ms, p95: ${percentile(homeProbes, 0.95)}ms`);
  console.log(`READER: probes = [${readerProbes.join(', ')}] -> p50: ${percentile(readerProbes, 0.5)}ms, p95: ${percentile(readerProbes, 0.95)}ms`);
  console.log(`MEDIA: probes = [${mediaProbes.join(', ')}] -> p50: ${percentile(mediaProbes, 0.5)}ms, p95: ${percentile(mediaProbes, 0.95)}ms`);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
