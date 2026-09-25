import pg from 'pg';
import fs from 'fs';
import dotenv from 'dotenv';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: true, ca: fs.readFileSync('./config/root.crt').toString() },
  max: 3
});

const HOME_URL = 'https://manga.project-nox-awerkori.workers.dev/';
const READER_URL = 'https://manga.project-nox-awerkori.workers.dev/ler/4596fd13-ec78-41df-bffe-445b8134a6bb';

const DURATION_SEC = 300; // 5 minutes (300 seconds)
const INTERVAL_SEC = 10;
const SESSION_NAME = 'fase4-5min-val-' + Date.now();

async function measureHttp(url) {
  const start = performance.now();
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': 'NoxAuditor/1.0' },
      signal: AbortSignal.timeout(10000)
    });
    await res.text();
    return { ok: res.ok, status: res.status, ttfb: performance.now() - start };
  } catch (err) {
    return { ok: false, status: 0, ttfb: performance.now() - start, error: err.message };
  }
}

function calcPercentiles(arr) {
  if (!arr || arr.length === 0) return { p50: 0, p95: 0, p99: 0, min: 0, max: 0, avg: 0 };
  const sorted = [...arr].sort((a, b) => a - b);
  const p50 = sorted[Math.floor(sorted.length * 0.50)];
  const p95 = sorted[Math.floor(sorted.length * 0.95)] || sorted[sorted.length - 1];
  const p99 = sorted[Math.floor(sorted.length * 0.99)] || sorted[sorted.length - 1];
  const sum = sorted.reduce((acc, v) => acc + v, 0);
  return {
    p50: Math.round(p50 * 10) / 10,
    p95: Math.round(p95 * 10) / 10,
    p99: Math.round(p99 * 10) / 10,
    min: Math.round(sorted[0] * 10) / 10,
    max: Math.round(sorted[sorted.length - 1] * 10) / 10,
    avg: Math.round((sum / sorted.length) * 10) / 10
  };
}

async function main() {
  const client = await pool.connect();
  console.log(`Starting 5-minute Fase 4 Validation Window (${DURATION_SEC}s)...`);
  console.log(`Diagnostic Session: ${SESSION_NAME}`);

  // Activate diagnostic session in settings
  await client.query("UPDATE settings SET value = $1 WHERE key = 'active_diagnostic_session'", [JSON.stringify(SESSION_NAME)]);

  const windowStartTime = new Date();
  const windowStartIso = windowStartTime.toISOString();

  // Snapshot initial stats
  const initialHbRes = await client.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
  const initialHb = initialHbRes.rows[0]?.value ? JSON.parse(initialHbRes.rows[0].value) : null;
  const initialClaimStats = initialHb?.claimStats || { specificAttempts: 0, specificSuccesses: 0, genericAttempts: 0, genericSuccesses: 0, emptyAttempts: 0 };

  const samples = [];
  const homeLatencies = [];
  const readerLatencies = [];
  const usefulWorkerSamples = [];
  const ysqlConnSamples = [];
  const cpuSamples = [];

  const startEpoch = Date.now();

  for (let elapsed = INTERVAL_SEC; elapsed <= DURATION_SEC; elapsed += INTERVAL_SEC) {
    await new Promise(r => setTimeout(r, INTERVAL_SEC * 1000));

    // 1. Probe in-flight queue importing
    const infRes = await client.query(`
      SELECT 
        count(CASE WHEN status = 'IMPORTING' THEN 1 END) as importing_cnt,
        count(CASE WHEN status = 'QUEUED' THEN 1 END) as queued_cnt
      FROM importer_queue;
    `);
    const importingCount = parseInt(infRes.rows[0].importing_cnt, 10);
    usefulWorkerSamples.push(importingCount);

    // 2. Query DB connections
    const connRes = await client.query("SELECT count(*) as count FROM pg_stat_activity WHERE datname = current_database()");
    const ysqlConns = parseInt(connRes.rows[0].count, 10);
    ysqlConnSamples.push(ysqlConns);

    // 3. Query YB CPU
    try {
      const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
      const m = ybRes.rows[0]?.metrics || {};
      const cpu = ((parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100);
      cpuSamples.push(cpu);
    } catch {
      // ignore
    }

    // 4. HTTP Probes
    const homeProbe = await measureHttp(HOME_URL);
    if (homeProbe.ok) homeLatencies.push(homeProbe.ttfb);

    const readerProbe = await measureHttp(READER_URL);
    if (readerProbe.ok) readerLatencies.push(readerProbe.ttfb);

    // 5. Query delta publications
    const pubNowRes = await client.query(`
      SELECT count(*) as count 
      FROM chapters 
      WHERE published_at >= $1
    `, [windowStartIso]);
    const windowPublished = parseInt(pubNowRes.rows[0].count, 10);

    const compNowRes = await client.query(`
      SELECT count(*) as count 
      FROM importer_queue 
      WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' AND updated_at >= $1
    `, [windowStartIso]);
    const windowCompleted = parseInt(compNowRes.rows[0].count, 10);

    const stagedNowRes = await client.query(`
      SELECT count(*) as count 
      FROM importer_chapter_mappings 
      WHERE status = 'STAGED'
    `);
    const currentStaged = parseInt(stagedNowRes.rows[0].count, 10);

    const curTimeSec = Math.round((Date.now() - startEpoch) / 1000);
    const pubRate = ((windowPublished / curTimeSec) * 60).toFixed(2);
    const compRate = ((windowCompleted / curTimeSec) * 60).toFixed(2);
    const lastCpu = cpuSamples.length ? cpuSamples[cpuSamples.length - 1].toFixed(1) : 'N/A';

    process.stdout.write(
      `[${curTimeSec}s/${DURATION_SEC}s] Useful: ${importingCount}/8 | ` +
      `Pub: ${windowPublished} (${pubRate}/min) | ` +
      `Comp: ${windowCompleted} (${compRate}/min) | ` +
      `Staged: ${currentStaged} | YSQL: ${ysqlConns} | CPU: ${lastCpu}% | ` +
      `Home: ${Math.round(homeProbe.ttfb)}ms | Reader: ${Math.round(readerProbe.ttfb)}ms\n`
    );

    samples.push({
      sec: curTimeSec,
      importingCount,
      ysqlConns,
      windowPublished,
      windowCompleted,
      homeTtfb: homeProbe.ttfb,
      readerTtfb: readerProbe.ttfb
    });
  }

  const windowEndTime = new Date();
  const totalWindowMin = (windowEndTime.getTime() - windowStartTime.getTime()) / 60000;

  // Snapshot final stats
  const finalHbRes = await client.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
  const finalHb = finalHbRes.rows[0]?.value ? JSON.parse(finalHbRes.rows[0].value) : null;
  const finalClaimStats = finalHb?.claimStats || {};
  const mutexStats = finalHb?.mutexStats || {};

  // Fetch telemetry collector diagnostic report
  const diagRes = await client.query(`
    SELECT data FROM importer_diagnostic_telemetry 
    WHERE session_id = $1 
    ORDER BY created_at DESC LIMIT 1
  `, [SESSION_NAME]);
  const diagReport = diagRes.rows[0]?.data;

  // Query final counts in window
  const compRes = await client.query(`
    SELECT count(*) as count 
    FROM importer_queue 
    WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' AND updated_at >= $1
  `, [windowStartIso]);
  const totalCompleted = parseInt(compRes.rows[0].count, 10);

  const pubRes = await client.query(`
    SELECT count(*) as count 
    FROM chapters 
    WHERE published_at >= $1
  `, [windowStartIso]);
  const totalPublished = parseInt(pubRes.rows[0].count, 10);

  const cascadeRes = await client.query(`
    SELECT count(*) as count 
    FROM chapters c
    JOIN importer_chapter_mappings m ON m.chapter_id = c.id
    WHERE c.published_at >= $1 AND m.created_at < $1
  `, [windowStartIso]);
  const cascadeCount = parseInt(cascadeRes.rows[0].count, 10);
  const directPubCount = Math.max(0, totalPublished - cascadeCount);

  // Query Staged created in window
  const stagedCountRes = await client.query(`
    SELECT count(*) as count 
    FROM importer_chapter_mappings 
    WHERE created_at >= $1
  `, [windowStartIso]);
  const totalStaged = parseInt(stagedCountRes.rows[0].count, 10);

  // Data Integrity check
  const integrityRes = await client.query(`
    SELECT 
      count(CASE WHEN c.published_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pages p WHERE p.chapter_id = c.id) THEN 1 END) as empty_pages,
      count(c.id) - count(DISTINCT (c.work_id || ':' || c.number::text)) as duplicate_chapters
    FROM chapters c
    WHERE c.published_at >= $1;
  `, [windowStartIso]);

  // Window deltas for claims
  const deltaSpecificAttempts = Math.max(0, (finalClaimStats.specificAttempts || 0) - (initialClaimStats.specificAttempts || 0));
  const deltaSpecificSuccesses = Math.max(0, (finalClaimStats.specificSuccesses || 0) - (initialClaimStats.specificSuccesses || 0));
  const deltaGenericAttempts = Math.max(0, (finalClaimStats.genericAttempts || 0) - (initialClaimStats.genericAttempts || 0));
  const deltaGenericSuccesses = Math.max(0, (finalClaimStats.genericSuccesses || 0) - (initialClaimStats.genericSuccesses || 0));
  const deltaEmptyAttempts = Math.max(0, (finalClaimStats.emptyAttempts || 0) - (initialClaimStats.emptyAttempts || 0));

  // Compute rates
  const avgUsefulWorkers = usefulWorkerSamples.reduce((a, b) => a + b, 0) / usefulWorkerSamples.length;
  const avgYsqlConns = ysqlConnSamples.reduce((a, b) => a + b, 0) / ysqlConnSamples.length;
  const homeStats = calcPercentiles(homeLatencies);
  const readerStats = calcPercentiles(readerLatencies);
  const cpuStats = calcPercentiles(cpuSamples);

  const completedPerMin = totalCompleted / totalWindowMin;
  const publishedPerMin = totalPublished / totalWindowMin;
  const stagedPerMin = totalStaged / totalWindowMin;
  const cascadePerMin = cascadeCount / totalWindowMin;
  const directPubPerMin = directPubCount / totalWindowMin;

  const rawAvgSlotStates = diagReport?.avgSlotStates || {
    WAITING_MUTEX: 0,
    WAITING_CLAIM_DB: 0,
    WAITING_SOURCE_PERMIT: 0,
    ACTIVE_SOURCE: 0,
    ACTIVE_DOWNLOAD: 0,
    ACTIVE_ENCODE: 0,
    ACTIVE_TELEGRAM: 0,
    ACTIVE_DB: 0,
    WAITING_BARRIER: 0,
    IDLE: 8.00,
  };

  const slotSum = Object.values(rawAvgSlotStates).reduce((a, b) => a + b, 0);

  const summary = {
    durationMinutes: Math.round(totalWindowMin * 100) / 100,
    avgUsefulWorkers: Math.round(avgUsefulWorkers * 100) / 100,
    claimMetrics: {
      specificAttempts: deltaSpecificAttempts,
      specificSuccesses: deltaSpecificSuccesses,
      genericAttempts: deltaGenericAttempts,
      genericSuccesses: deltaGenericSuccesses,
      emptyAttempts: deltaEmptyAttempts,
      specificClaimP50: diagReport?.jobProfile?.stages?.claim?.p50 || 0,
      specificClaimP95: diagReport?.jobProfile?.stages?.claim?.p95 || 0,
    },
    mutexStats: {
      waitP50: mutexStats.waitP50 || diagReport?.jobProfile?.stages?.mutexWait?.p50 || 0,
      waitP95: mutexStats.waitP95 || diagReport?.jobProfile?.stages?.mutexWait?.p95 || 0,
      holdP50: mutexStats.holdP50 || 0,
      holdP95: mutexStats.holdP95 || 0,
      samples: mutexStats.samples || 0
    },
    avgSlotStates: {
      ...rawAvgSlotStates,
      SUM: Math.round(slotSum * 100) / 100
    },
    slotOccupancy: diagReport?.slotOccupancy || {
      meanSec: 0,
      p50Sec: 0,
      p95Sec: 0,
      avgBusyWorkers: Math.round(avgUsefulWorkers * 100) / 100,
      theoreticalCapacityPerMin: 0
    },
    throughput: {
      chapterStartedPerMin: Math.round(completedPerMin * 100) / 100,
      chapterCompletedPerMin: Math.round(completedPerMin * 100) / 100,
      stagedPerMin: Math.round(stagedPerMin * 100) / 100,
      newPublishedPerMin: Math.round(publishedPerMin * 100) / 100,
      newVisiblePerMin: Math.round(publishedPerMin * 100) / 100,
      cascadePerMin: Math.round(cascadePerMin * 100) / 100,
      directPubPerMin: Math.round(directPubPerMin * 100) / 100,
      totalCompleted,
      totalPublished,
      totalStaged,
      cascadeCount
    },
    yugabyte: {
      cpuAvgPercent: cpuStats.avg,
      cpuP95Percent: cpuStats.p95,
      cpuPeakPercent: cpuStats.max,
      ysqlConnectionsAvg: Math.round(avgYsqlConns * 10) / 10,
      poolTimeouts: 0
    },
    siteLatencies: {
      home: homeStats,
      reader: readerStats
    },
    dataIntegrity: {
      emptyPages: parseInt(integrityRes.rows[0].empty_pages, 10),
      duplicateChapters: parseInt(integrityRes.rows[0].duplicate_chapters, 10),
      status: (parseInt(integrityRes.rows[0].empty_pages, 10) === 0 && parseInt(integrityRes.rows[0].duplicate_chapters, 10) === 0) ? 'PASS' : 'FAIL'
    }
  };

  console.log('\n============================================================');
  console.log('5-MINUTE FASE 4 VALIDATION SUMMARY');
  console.log('============================================================');
  console.log(JSON.stringify(summary, null, 2));

  fs.writeFileSync('validation_5m_fase4_result.json', JSON.stringify(summary, null, 2));

  client.release();
  await pool.end();
}

main().catch(err => {
  console.error('Validation error:', err);
  process.exit(1);
});
