import pg from 'pg';
import dotenv from 'dotenv';
import https from 'https';
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

const agent = new https.Agent({ keepAlive: true, maxSockets: 10 });
const HOME_URL = 'https://manga.project-nox-awerkori.workers.dev/';
const READER_URL = 'https://manga.project-nox-awerkori.workers.dev/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c';
const MEDIA_URL = 'https://manga.project-nox-awerkori.workers.dev/media/000003ed-c2db-4794-bcfa-c5e8b21ce080';

function measureTTFB(url) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = https.get(url, { agent }, (res) => {
      let resolved = false;
      res.once('data', () => {
        if (!resolved) {
          resolved = true;
          const ttfb = Math.round(performance.now() - t0);
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
    req.setTimeout(4000, () => { req.destroy(); resolve({ status: 408, ttfb: 9999 }); });
  });
}

async function run() {
  await client.connect();

  console.log('=== COLLECTING COMPREHENSIVE PRODUCTION METRICS ===\n');

  // 1. Settings & Feature Flags
  const settingsRes = await client.query("SELECT key, value FROM settings WHERE key IN ('work_affinity_scheduler_enabled', 'work_affinity_scheduler_shadow', 'importer_protective_stop')");
  console.log('--- 1. SETTINGS & FEATURE FLAGS ---');
  settingsRes.rows.forEach(r => console.log(`${r.key}: ${r.value}`));

  // 2. Queue Breakdown
  const queueRes = await client.query("SELECT status, count(*) FROM importer_queue GROUP BY status ORDER BY count(*) DESC");
  console.log('\n--- 2. IMPORTER QUEUE STATUS ---');
  queueRes.rows.forEach(r => console.log(`${r.status}: ${r.count}`));

  // 3. Scheduler State & Active Works
  const stateRes = await client.query("SELECT key, value FROM importer_scheduler_state");
  const stateMap = {};
  stateRes.rows.forEach(r => stateMap[r.key] = r.value);
  console.log('\n--- 3. SCHEDULER STATE & METRICS ---');
  console.log('Metrics:', JSON.stringify(stateMap['metrics'], null, 2));
  console.log('Active Works Count:', Array.isArray(stateMap['active_works']) ? stateMap['active_works'].length : 0);
  if (Array.isArray(stateMap['active_works'])) {
    stateMap['active_works'].forEach((w, i) => {
      console.log(`  [${i+1}] ${w.workTitle || w.workId} | Lane: ${w.lane} | State: ${w.state} | Source: ${w.primarySource} | Pub: ${w.publishedChapters} | Queued: ${w.queuedChapters} | InFlight: ${w.inFlightChapters} | Frontier: ${w.frontierSortKey}`);
    });
  }

  // 4. Publication Throughput (10m, 1h, 24h)
  const pub10m = await client.query("SELECT count(*) FROM chapters WHERE published_at >= NOW() - INTERVAL '10 minutes'");
  const pub1h = await client.query("SELECT count(*) FROM chapters WHERE published_at >= NOW() - INTERVAL '1 hour'");
  const pub24h = await client.query("SELECT count(*) FROM chapters WHERE published_at >= NOW() - INTERVAL '24 hours'");
  const capPerMin10m = (parseInt(pub10m.rows[0].count, 10) / 10).toFixed(2);
  const capPerMin1h = (parseInt(pub1h.rows[0].count, 10) / 60).toFixed(2);
  console.log('\n--- 4. PUBLICATION THROUGHPUT ---');
  console.log(`Last 10m: ${pub10m.rows[0].count} chapters (${capPerMin10m} cap/min)`);
  console.log(`Last 1h:  ${pub1h.rows[0].count} chapters (${capPerMin1h} cap/min)`);
  console.log(`Last 24h: ${pub24h.rows[0].count} chapters`);

  // 5. Duplicates Check
  const dupesRes = await client.query(`
    SELECT work_id, number, count(*) 
    FROM chapters 
    WHERE published_at >= NOW() - INTERVAL '24 hours'
    GROUP BY work_id, number 
    HAVING count(*) > 1
  `);
  console.log('\n--- 5. DUPLICATE CHECK ---');
  console.log(`Duplicate publications in last 24h: ${dupesRes.rows.length}`);

  // 6. DB Connections
  const connsRes = await client.query(`
    SELECT count(*)::int as total,
           count(*) FILTER (WHERE state = 'active')::int as active,
           count(*) FILTER (WHERE state = 'idle')::int as idle
    FROM pg_stat_activity
  `);
  console.log('\n--- 6. YSQL CONNECTIONS ---');
  console.log(connsRes.rows[0]);

  // 7. Edge TTFB
  console.log('\n--- 7. EDGE TTFB LATENCIES (keep-alive) ---');
  await measureTTFB(HOME_URL);
  const h = await measureTTFB(HOME_URL);
  const r = await measureTTFB(READER_URL);
  const m = await measureTTFB(MEDIA_URL);
  console.log(`Home TTFB:   ${h.ttfb}ms (status: ${h.status})`);
  console.log(`Reader TTFB: ${r.ttfb}ms (status: ${r.status})`);
  console.log(`Media TTFB:  ${m.ttfb}ms (status: ${m.status})`);

  await client.end();
}

run().catch(console.error);
