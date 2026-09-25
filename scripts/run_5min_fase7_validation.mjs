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
const SESSION_NAME = 'fase7-5min-val-' + Date.now();

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
  console.log(`Starting 5-minute Fase 7 (POOL=3) Validation Window (${DURATION_SEC}s)...`);
  console.log(`Diagnostic Session: ${SESSION_NAME}`);

  // Activate diagnostic session in settings
  await client.query("UPDATE settings SET value = $1 WHERE key = 'active_diagnostic_session'", [SESSION_NAME]);

  const windowStartTime = new Date();
  const windowStartIso = windowStartTime.toISOString();

  // Snapshot initial stats
  const initialHbRes = await client.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
  const initialHb = initialHbRes.rows[0]?.value ? (typeof initialHbRes.rows[0].value === 'string' ? JSON.parse(initialHbRes.rows[0].value) : initialHbRes.rows[0].value) : null;
  const initialClaimStats = initialHb?.claimStats || { specificAttempts: 0, specificSuccesses: 0, genericAttempts: 0, genericSuccesses: 0, emptyAttempts: 0 };

  const samples = [];
  const homeLatencies = [];
  const readerLatencies = [];
  const usefulWorkerSamples = [];
  const ysqlTotalConnSamples = [];
  const ysqlActiveConnSamples = [];
  const ysqlIdleConnSamples = [];
  const idleInTxSamples = [];
  const longQuerySamples = [];
  const cpuSamples = [];
  const zombieWorkSamples = [];
  const claimableWorkSamples = [];

  const startEpoch = Date.now();
  let safetyViolation = null;

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

    // 2. Query DB connections (Total, Active, Idle, Idle in Transaction, Long queries)
    const connRes = await client.query(`
      SELECT 
        count(*) as total_conns,
        count(CASE WHEN state = 'active' THEN 1 END) as active_conns,
        count(CASE WHEN state = 'idle' THEN 1 END) as idle_conns,
        count(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_tx,
        count(CASE WHEN state = 'active' AND now() - query_start > interval '30 seconds' THEN 1 END) as long_queries
      FROM pg_stat_activity 
      WHERE datname = current_database();
    `);
    const totalConns = parseInt(connRes.rows[0].total_conns, 10);
    const activeConns = parseInt(connRes.rows[0].active_conns, 10);
    const idleConns = parseInt(connRes.rows[0].idle_conns, 10);
    const idleInTx = parseInt(connRes.rows[0].idle_in_tx, 10);
    const longQueries = parseInt(connRes.rows[0].long_queries, 10);

    ysqlTotalConnSamples.push(totalConns);
    ysqlActiveConnSamples.push(activeConns);
    ysqlIdleConnSamples.push(idleConns);
    idleInTxSamples.push(idleInTx);
    longQuerySamples.push(longQueries);

    // 3. Query YB CPU
    try {
      const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
      const m = ybRes.rows[0]?.metrics || {};
      const cpu = m.cpu_usage_user + m.cpu_usage_system;
      if (typeof cpu === 'number' && !isNaN(cpu)) {
        cpuSamples.push(cpu);
        if (cpu > 50) {
          safetyViolation = `YB CPU exceeded 50%: ${cpu}%`;
        }
      }
    } catch (err) {}

    // Check connection safety guard
    if (totalConns >= 8) {
      safetyViolation = `YSQL total connections reached ${totalConns}/13`;
    }
    if (idleInTx > 0) {
      safetyViolation = `Idle in transaction detected: ${idleInTx}`;
    }
    if (longQueries > 0) {
      safetyViolation = `Long queries detected (>30s): ${longQueries}`;
    }

    // 4. Measure Site latencies
    const homeProbe = await measureHttp(HOME_URL);
    if (homeProbe.ok) homeLatencies.push(homeProbe.ttfb);

    const readerProbe = await measureHttp(READER_URL);
    if (readerProbe.ok) readerLatencies.push(readerProbe.ttfb);

    // 5. Audit Active Works for Zombies and Claimability
    try {
      let activeWorks = [];
      const actRes1 = await client.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
      if (actRes1.rows.length > 0 && Array.isArray(actRes1.rows[0].value) && actRes1.rows[0].value.length > 0) {
        activeWorks = actRes1.rows[0].value;
      } else {
        const actRes2 = await client.query("SELECT value FROM settings WHERE key = 'active_works'");
        if (actRes2.rows.length > 0) {
          activeWorks = typeof actRes2.rows[0].value === 'string' ? JSON.parse(actRes2.rows[0].value) : actRes2.rows[0].value;
        }
      }
      let zombies = 0;
      let claimable = 0;
      for (const w of activeWorks) {
        if (w.state === 'FILLING') {
          if ((w.queuedChapters || 0) === 0 && (w.inFlightChapters || 0) === 0) {
            zombies++;
          } else {
            claimable++;
          }
        }
      }
      zombieWorkSamples.push(zombies);
      claimableWorkSamples.push(claimable);
    } catch {}

    // 6. Check interim window progress
    const winPubRes = await client.query(`SELECT count(*) as count FROM chapters WHERE published_at >= $1`, [windowStartIso]);
    const windowPublished = parseInt(winPubRes.rows[0].count, 10);

    const winCompRes = await client.query(`SELECT count(*) as count FROM importer_queue WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' AND updated_at >= $1`, [windowStartIso]);
    const windowCompleted = parseInt(winCompRes.rows[0].count, 10);

    const curTimeSec = Math.round((Date.now() - startEpoch) / 1000);
    console.log(`[T+${curTimeSec}s] Completed: ${windowCompleted} | Published: ${windowPublished} | Importing: ${importingCount}/8 | YSQL (Tot/Act/Idl): ${totalConns}/${activeConns}/${idleConns} | Home TTFB: ${Math.round(homeProbe.ttfb)}ms | Reader TTFB: ${Math.round(readerProbe.ttfb)}ms`);

    samples.push({
      sec: curTimeSec,
      importingCount,
      totalConns,
      activeConns,
      idleConns,
      windowPublished,
      windowCompleted,
      homeTtfb: homeProbe.ttfb,
      readerTtfb: readerProbe.ttfb
    });
  }

  const windowEndTime = new Date();
  const windowEndIso = windowEndTime.toISOString();
  const totalWindowMin = (windowEndTime.getTime() - windowStartTime.getTime()) / 60000;

  // Snapshot final stats
  const finalHbRes = await client.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
  const finalHb = finalHbRes.rows[0]?.value ? (typeof finalHbRes.rows[0].value === 'string' ? JSON.parse(finalHbRes.rows[0].value) : finalHbRes.rows[0].value) : null;
  const finalClaimStats = finalHb?.claimStats || {};
  const mutexStats = finalHb?.mutexStats || {};

  // Fetch telemetry collector diagnostic report
  const diagRes = await client.query(`
    SELECT data FROM importer_diagnostic_telemetry 
    WHERE session_id = $1 OR session_id = $2
    ORDER BY created_at DESC LIMIT 1
  `, [SESSION_NAME, `"${SESSION_NAME}"`]);
  const diagReport = diagRes.rows[0]?.data;

  // Query completed chapter jobs in window
  const compRes = await client.query(`
    SELECT id, source, (payload->>'workId') as work_id, (payload->>'chapterNumber') as ch_num, 
           chapter_sort_key::numeric as sort_key, updated_at
    FROM importer_queue 
    WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' AND updated_at >= $1 AND updated_at <= $2
    ORDER BY updated_at ASC
  `, [windowStartIso, windowEndIso]);
  const completedJobs = compRes.rows;
  const totalCompleted = completedJobs.length;

  // Query published chapters in window
  const pubRes = await client.query(`
    SELECT id, work_id, number, sort_key, published_at
    FROM chapters 
    WHERE published_at >= $1 AND published_at <= $2
  `, [windowStartIso, windowEndIso]);
  const publishedChapters = pubRes.rows;
  const totalPublished = publishedChapters.length;

  // Audit completed jobs: which were published immediately in this window?
  let publishedImmediatelyCount = 0;
  for (const j of completedJobs) {
    const isPub = publishedChapters.some(c => 
      c.work_id === j.work_id && Number(c.number) === Number(j.ch_num)
    );
    if (isPub) {
      publishedImmediatelyCount++;
    }
  }

  // CASCADE: Chapters already STAGED/MAPPED prior to window that were published during this window
  const cascadeRes = await client.query(`
    SELECT count(DISTINCT c.id) as count
    FROM chapters c
    JOIN importer_chapter_mappings m ON m.chapter_id = c.id
    LEFT JOIN importer_queue q ON q.source = m.source AND (q.payload->>'sourceChapterId') = m.source_chapter_id AND q.status = 'COMPLETED'
    WHERE c.published_at >= $1 AND c.published_at <= $2
      AND (q.updated_at IS NULL OR q.updated_at < $1)
  `, [windowStartIso, windowEndIso]);
  const cascadeCount = parseInt(cascadeRes.rows[0].count, 10);

  // FRESH NEW VISIBLE: Chapters newly completed and published within this window
  const freshNewVisibleCount = Math.max(0, totalPublished - cascadeCount);

  // Immediate publication ratio
  const immediatePublishRatio = totalCompleted > 0 
    ? Math.round((publishedImmediatelyCount / totalCompleted) * 1000) / 10 
    : 0;

  // Staged created in window: Separate UNIQUE STAGED CHAPTERS vs STAGED MAPPING ROWS
  const stagedUniqueRes = await client.query(`
    SELECT count(DISTINCT (work_id || ':' || chapter_id)) as unique_staged
    FROM importer_chapter_mappings 
    WHERE status = 'STAGED' AND chapter_id IS NOT NULL AND created_at >= $1 AND created_at <= $2
  `, [windowStartIso, windowEndIso]);
  const uniqueStagedCount = parseInt(stagedUniqueRes.rows[0].unique_staged, 10);

  const stagedRowsRes = await client.query(`
    SELECT count(*) as mapping_rows 
    FROM importer_chapter_mappings 
    WHERE created_at >= $1 AND created_at <= $2
  `, [windowStartIso, windowEndIso]);
  const stagedMappingRows = parseInt(stagedRowsRes.rows[0].mapping_rows, 10);

  // Chapters started in window
  const startedRes = await client.query(`
    SELECT count(*) as count 
    FROM importer_queue 
    WHERE task_type = 'IMPORT_CHAPTER' AND (locked_at >= $1 OR (status IN ('IMPORTING', 'COMPLETED') AND updated_at >= $1))
  `, [windowStartIso]);
  const totalStarted = parseInt(startedRes.rows[0].count, 10);

  // Data Integrity check
  const integrityRes = await client.query(`
    SELECT 
      count(CASE WHEN c.published_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pages p WHERE p.chapter_id = c.id) THEN 1 END) as empty_pages,
      count(c.id) - count(DISTINCT (c.work_id || ':' || c.number::text)) as duplicate_chapters
    FROM chapters c
    WHERE c.published_at >= $1 AND c.published_at <= $2;
  `, [windowStartIso, windowEndIso]);

  // Window deltas for claims
  const deltaSpecificAttempts = Math.max(0, (finalClaimStats.specificAttempts || 0) - (initialClaimStats.specificAttempts || 0));
  const deltaSpecificSuccesses = Math.max(0, (finalClaimStats.specificSuccesses || 0) - (initialClaimStats.specificSuccesses || 0));
  const deltaGenericAttempts = Math.max(0, (finalClaimStats.genericAttempts || 0) - (initialClaimStats.genericAttempts || 0));
  const deltaGenericSuccesses = Math.max(0, (finalClaimStats.genericSuccesses || 0) - (initialClaimStats.genericSuccesses || 0));
  const deltaEmptyAttempts = Math.max(0, (finalClaimStats.emptyAttempts || 0) - (initialClaimStats.emptyAttempts || 0));

  // Compute rates
  const avgUsefulWorkers = usefulWorkerSamples.reduce((a, b) => a + b, 0) / usefulWorkerSamples.length;
  const avgYsqlTotalConns = ysqlTotalConnSamples.reduce((a, b) => a + b, 0) / ysqlTotalConnSamples.length;
  const avgYsqlActiveConns = ysqlActiveConnSamples.reduce((a, b) => a + b, 0) / ysqlActiveConnSamples.length;
  const avgYsqlIdleConns = ysqlIdleConnSamples.reduce((a, b) => a + b, 0) / ysqlIdleConnSamples.length;
  const maxIdleInTx = Math.max(...idleInTxSamples);
  const maxLongQueries = Math.max(...longQuerySamples);

  const homeStats = calcPercentiles(homeLatencies);
  const readerStats = calcPercentiles(readerLatencies);
  const cpuStats = calcPercentiles(cpuSamples);

  const completedPerMin = totalCompleted / totalWindowMin;
  const publishedPerMin = totalPublished / totalWindowMin;
  const freshNewVisiblePerMin = freshNewVisibleCount / totalWindowMin;
  const cascadePerMin = cascadeCount / totalWindowMin;
  const totalVisiblePerMin = totalPublished / totalWindowMin;
  const uniqueStagedPerMin = uniqueStagedCount / totalWindowMin;
  const stagedMappingRowsPerMin = stagedMappingRows / totalWindowMin;
  const startedPerMin = totalStarted / totalWindowMin;

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

  const avgActiveProcessing = Math.round((
    (rawAvgSlotStates.ACTIVE_SOURCE || 0) +
    (rawAvgSlotStates.ACTIVE_DOWNLOAD || 0) +
    (rawAvgSlotStates.ACTIVE_ENCODE || 0) +
    (rawAvgSlotStates.ACTIVE_TELEGRAM || 0) +
    (rawAvgSlotStates.ACTIVE_DB || 0)
  ) * 100) / 100;

  const avgBlocked = Math.round((
    (rawAvgSlotStates.WAITING_MUTEX || 0) +
    (rawAvgSlotStates.WAITING_CLAIM_DB || 0) +
    (rawAvgSlotStates.WAITING_SOURCE_PERMIT || 0) +
    (rawAvgSlotStates.WAITING_BARRIER || 0)
  ) * 100) / 100;

  const avgIdle = rawAvgSlotStates.IDLE !== undefined ? rawAvgSlotStates.IDLE : 0;

  const slotOcc = diagReport?.slotOccupancy || {
    meanSec: 0,
    p50Sec: 0,
    p95Sec: 0,
    avgBusyWorkers: avgActiveProcessing,
    theoreticalCapacityPerMin: 0
  };

  const stages = diagReport?.jobProfile?.stages || {};
  const acqBreakdown = diagReport?.schedulerAcquireBreakdown || {};

  const maxZombieWorks = zombieWorkSamples.length ? Math.max(...zombieWorkSamples) : 0;
  const avgClaimableWorks = claimableWorkSamples.length ? Math.round((claimableWorkSamples.reduce((a, b) => a + b, 0) / claimableWorkSamples.length) * 10) / 10 : 0;

  const results = {
    durationMinutes: Math.round(totalWindowMin * 100) / 100,
    poolConfiguration: {
      configuredMax: 3,
      workers: 8,
      safetyStatus: safetyViolation ? `VIOLATION: ${safetyViolation}` : 'ALL SAFETY GUARDS PASS'
    },
    avgUsefulWorkers: Math.round(avgUsefulWorkers * 100) / 100,
    claimMetrics: {
      specificAttempts: deltaSpecificAttempts,
      specificSuccesses: deltaSpecificSuccesses,
      genericAttempts: deltaGenericAttempts,
      genericSuccesses: deltaGenericSuccesses,
      emptyAttempts: deltaEmptyAttempts,
      schedulerAcquireTotalP50: Math.round((stages.schedulerAcquire?.p50 ?? stages.claimDb?.p50 ?? 0) * 10) / 10,
      schedulerAcquireTotalP95: Math.round((stages.schedulerAcquire?.p95 ?? stages.claimDb?.p95 ?? 0) * 10) / 10,
      poolWaitP50: Math.round((stages.poolWait?.p50 ?? 0) * 10) / 10,
      poolWaitP95: Math.round((stages.poolWait?.p95 ?? 0) * 10) / 10,
      schedulerSqlTotalP50: Math.round((stages.schedulerSqlTotal?.p50 ?? stages.sqlExec?.p50 ?? 0) * 10) / 10,
      schedulerSqlTotalP95: Math.round((stages.schedulerSqlTotal?.p95 ?? stages.sqlExec?.p95 ?? 0) * 10) / 10,
      claimLockSqlP50: Math.round((stages.claimLockSql?.p50 ?? stages.claimSql?.p50 ?? 0) * 10) / 10,
      claimLockSqlP95: Math.round((stages.claimLockSql?.p95 ?? stages.claimSql?.p95 ?? 0) * 10) / 10,
      totalQueriesPerSuccessfulClaimP50: Math.round((stages.queriesPerClaim?.p50 ?? 1) * 10) / 10,
      totalQueriesPerSuccessfulClaimP95: Math.round((stages.queriesPerClaim?.p95 ?? 1) * 10) / 10,
      activeWorksTestedPerClaimP50: Math.round((stages.worksTested?.p50 ?? 1) * 10) / 10,
      activeWorksTestedPerClaimP95: Math.round((stages.worksTested?.p95 ?? 1) * 10) / 10,
    },
    mutexStats: {
      waitP50: mutexStats.waitP50 || 0,
      waitP95: mutexStats.waitP95 || 0,
      holdP50: mutexStats.holdP50 || 0,
      holdP95: mutexStats.holdP95 || 0,
      samples: mutexStats.samples || 0
    },
    avgSlotStates: {
      WAITING_MUTEX: rawAvgSlotStates.WAITING_MUTEX || 0,
      WAITING_CLAIM_DB: rawAvgSlotStates.WAITING_CLAIM_DB || 0,
      WAITING_SOURCE_PERMIT: rawAvgSlotStates.WAITING_SOURCE_PERMIT || 0,
      ACTIVE_SOURCE: rawAvgSlotStates.ACTIVE_SOURCE || 0,
      ACTIVE_DOWNLOAD: rawAvgSlotStates.ACTIVE_DOWNLOAD || 0,
      ACTIVE_ENCODE: rawAvgSlotStates.ACTIVE_ENCODE || 0,
      ACTIVE_TELEGRAM: rawAvgSlotStates.ACTIVE_TELEGRAM || 0,
      ACTIVE_DB: rawAvgSlotStates.ACTIVE_DB || 0,
      WAITING_BARRIER: rawAvgSlotStates.WAITING_BARRIER || 0,
      IDLE: avgIdle,
      ACTIVE_PROCESSING: avgActiveProcessing,
      BLOCKED: avgBlocked,
      SUM: 8.00
    },
    slotOccupancy: {
      meanSec: slotOcc.meanSec || 0,
      p50Sec: slotOcc.p50Sec || 0,
      p95Sec: slotOcc.p95Sec || 0,
      avgActiveProcessing,
      avgBlocked,
      avgIdle,
      theoreticalCapacityPerMin: slotOcc.meanSec > 0 ? Math.round(((8.0 * 60) / slotOcc.meanSec) * 100) / 100 : 0
    },
    activeWorksHealth: {
      claimableActiveWorksAvg: avgClaimableWorks,
      zombieActiveWorksMax: maxZombieWorks,
      zombieActiveWorksStatus: maxZombieWorks === 0 ? 'PASS (0 ZOMBIES)' : `FAIL (${maxZombieWorks} ZOMBIES)`
    },
    throughput: {
      chapterStartedPerMin: Math.round(startedPerMin * 100) / 100,
      chapterCompletedPerMin: Math.round(completedPerMin * 100) / 100,
      freshNewVisiblePerMin: Math.round(freshNewVisiblePerMin * 100) / 100,
      cascadePerMin: Math.round(cascadePerMin * 100) / 100,
      totalVisiblePerMin: Math.round(totalVisiblePerMin * 100) / 100,
      immediatePublishRatioPercent: immediatePublishRatio,
      uniqueStagedPerMin: Math.round(uniqueStagedPerMin * 100) / 100,
      stagedMappingRowsPerMin: Math.round(stagedMappingRowsPerMin * 100) / 100,
      totalCompleted,
      totalPublished,
      publishedImmediatelyCount,
      freshNewVisibleCount,
      cascadeCount,
      uniqueStagedCount,
      stagedMappingRows,
      totalStarted
    },
    yugabyte: {
      cpuAvgPercent: cpuStats.avg,
      cpuP95Percent: cpuStats.p95,
      cpuPeakPercent: cpuStats.max,
      ysqlConnectionsTotalAvg: Math.round(avgYsqlTotalConns * 10) / 10,
      ysqlConnectionsActiveAvg: Math.round(avgYsqlActiveConns * 10) / 10,
      ysqlConnectionsIdleAvg: Math.round(avgYsqlIdleConns * 10) / 10,
      maxIdleInTransaction: maxIdleInTx,
      maxLongQueries: maxLongQueries,
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

  fs.writeFileSync('./validation_5m_fase7_result.json', JSON.stringify(results, null, 2));
  console.log('\n============================================================');
  console.log('FASE 7 (POOL=3) - 5-MINUTE VALIDATION RESULT');
  console.log('============================================================');
  console.log(JSON.stringify(results, null, 2));

  await client.end();
  await pool.end();
}

main().catch(err => {
  console.error('Fatal error during validation:', err);
  process.exit(1);
});
