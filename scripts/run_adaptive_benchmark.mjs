import pg from 'pg';
import https from 'node:https';
import dotenv from 'dotenv';
import fs from 'node:fs';

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

function percentile(arr, p) {
  if (!arr || !arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
}

function avg(arr) {
  if (!arr || !arr.length) return 0;
  return parseFloat((arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1));
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

const ACTIVE_SOURCES = [
  'fleurblanche',
  'hanamiheaven',
  'mangalivreto',
  'manhastro',
  'taimumangas',
  'montetai',
  'mangaflix',
  'hotcabaretscan',
  'mangaonlinetv',
  'ninjascan',
  'mrtenzus',
  'nebulosascan'
];

async function main() {
  const stageName = process.argv[2] || 'stage-8w';
  const durationMinutes = parseFloat(process.argv[3] || '4.0');
  const configuredWorkers = parseInt(process.argv[4] || '8', 10);
  const sampleIntervalSec = parseInt(process.argv[5] || '10', 10);
  const durationMs = durationMinutes * 60 * 1000;

  const sessionId = `${stageName}-${Date.now()}`;

  console.log('======================================================================');
  console.log(`🚀 PROJECT NOX IMPORTER — BENCHMARK STAGE: ${stageName.toUpperCase()}`);
  console.log(`   Session ID: ${sessionId}`);
  console.log(`   Configured Workers: ${configuredWorkers}`);
  console.log(`   Duration: ${durationMinutes} minutes (${durationMs / 1000}s)`);
  console.log(`   Sources: ${ACTIVE_SOURCES.length} diverse active sources`);
  console.log(`   Datapath: DIRECT YSQL TLS -> YugabyteDB Aeon (Zero Gateway / Hyperdrive)`);
  console.log(`   Scope: IMPORT_CHAPTER only | Discovery: OFF | Safety Barrier: CONTROLLED`);
  console.log('======================================================================\n');

  const client = new Client(DB_CONFIG);
  await client.connect();

  const maxConnRes = await client.query('SHOW max_connections');
  const maxConnections = parseInt(maxConnRes.rows[0].max_connections, 10);

  // Warmup site probes
  console.log('[Setup] Warming up site HTTP keepAlive connections...');
  for (let i = 0; i < 3; i++) {
    await measureTTFB(HOME_URL, homeAgent);
    await measureTTFB(READER_URL, readerAgent);
    await measureTTFB(MEDIA_URL, mediaAgent);
    await new Promise(r => setTimeout(r, 100));
  }

  // Ensure discovery is disabled
  console.log('[Setup] Enforcing global catalog discovery DISABLED...');
  await client.query(`
    INSERT INTO settings (key, value)
    VALUES ('catalog_discovery_enabled', 'DISABLED')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
  `);

  // Activate participating sources
  console.log(`[Setup] Activating ${ACTIVE_SOURCES.length} chapter sources...`);
  await client.query(`
    UPDATE importer_sources 
    SET chapter_ingestion_enabled = true,
        catalog_discovery_enabled = false,
        enabled = true,
        blocked_reason = NULL,
        status = 'ACTIVE',
        updated_at = NOW() 
    WHERE id = ANY($1)
  `, [ACTIVE_SOURCES]);

  const dbTimeRes = await client.query('SELECT NOW() as db_start_time');
  const benchmarkStartTimeIso = dbTimeRes.rows[0].db_start_time.toISOString();
  console.log(`[Setup] Benchmark Start Time (DB UTC): ${benchmarkStartTimeIso}`);

  // Register active diagnostic session in settings
  console.log(`[Setup] Registering active diagnostic session [${sessionId}] in settings...`);
  await client.query(`
    INSERT INTO settings (key, value)
    VALUES ('active_diagnostic_session', $1)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
  `, [sessionId]);

  // Open Publication Safety Barrier
  console.log('[Setup] Opening Publication Safety Barrier...');
  await client.query("UPDATE settings SET value = 'OPEN' WHERE key = 'publication_safety_barrier'");
  console.log('✅ Publication Safety Barrier OPEN. Workers actively processing jobs.\n');

  const startTime = Date.now();
  const samples = [];
  const homeLatencies = [];
  const readerLatencies = [];
  const mediaLatencies = [];
  let totalSiteProbes = 0;
  let siteErrors = 0;

  const ysqlConnSamples = [];
  const ybCpuSamples = [];
  const discloudCpuSamples = [];
  const discloudRamSamples = [];
  const effectiveActiveSamples = [];

  let emergencyTriggered = false;
  let emergencyReason = '';
  let sampleIdx = 0;

  try {
    while (Date.now() - startTime < durationMs) {
      sampleIdx++;
      const t0 = Date.now();
      const elapsedSec = Math.round((Date.now() - startTime) / 1000);

      // 1. Database activity snapshot
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

      // Yugabyte CPU
      const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
      const m = ybRes.rows[0]?.metrics || {};
      const cpu = (parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100;
      ybCpuSamples.push(cpu);

      // 2. Queue state
      const queueRes = await client.query(`
        SELECT count(*) FILTER (WHERE status = 'IMPORTING') as importing_now,
               count(*) FILTER (WHERE status = 'QUEUED') as queued_now
        FROM importer_queue
        WHERE task_type = 'IMPORT_CHAPTER'
      `);
      const importingNow = parseInt(queueRes.rows[0].importing_now, 10);
      const queuedNow = parseInt(queueRes.rows[0].queued_now, 10);
      effectiveActiveSamples.push(importingNow);

      // 3. Completed chapters from importer_queue & media
      const compRes = await client.query(`
        SELECT count(*) as completed_count,
               COALESCE(sum(progress_current), 0) as completed_pages
        FROM importer_queue
        WHERE task_type = 'IMPORT_CHAPTER'
          AND status = 'COMPLETED'
          AND updated_at >= $1
      `, [benchmarkStartTimeIso]);
      const completedChapters = parseInt(compRes.rows[0].completed_count, 10);
      const totalPages = parseInt(compRes.rows[0].completed_pages, 10);

      const mediaRes = await client.query(`
        SELECT count(*) as media_count,
               COALESCE(sum(bytes), 0) as total_bytes
        FROM media
        WHERE created_at >= $1
      `, [benchmarkStartTimeIso]);
      const mediaCount = parseInt(mediaRes.rows[0].media_count, 10);
      const totalBytes = parseInt(mediaRes.rows[0].total_bytes, 10);
      const totalBytesMb = (totalBytes / (1024 * 1024)).toFixed(1);

      const elapsedMinutes = elapsedSec > 0 ? elapsedSec / 60 : 0.01;
      const pagesPerMin = (totalPages / elapsedMinutes).toFixed(1);
      const capPerMin = (completedChapters / elapsedMinutes).toFixed(1);
      const mbPerMin = (parseFloat(totalBytesMb) / elapsedMinutes).toFixed(1);

      // 4. Site latency probes
      const homeRes = await measureTTFB(HOME_URL, homeAgent);
      const readerRes = await measureTTFB(READER_URL, readerAgent);
      const mediaProbeRes = await measureTTFB(MEDIA_URL, mediaAgent);

      totalSiteProbes += 3;
      if (homeRes.status >= 400 || homeRes.error) siteErrors++;
      else if (homeRes.ttfb < 9000) homeLatencies.push(homeRes.ttfb);

      if (readerRes.status >= 400 || readerRes.error) siteErrors++;
      else if (readerRes.ttfb < 9000) readerLatencies.push(readerRes.ttfb);

      if (mediaProbeRes.status >= 400 || mediaProbeRes.error) siteErrors++;
      else if (mediaProbeRes.ttfb < 9000) mediaLatencies.push(mediaProbeRes.ttfb);

      // 5. Discloud live status
      const discloud = await getDiscloudStatus();
      if (discloud.cpu > 0) discloudCpuSamples.push(discloud.cpu);
      if (discloud.ram > 0) discloudRamSamples.push(discloud.ram);

      // 6. Importer internal diagnostic telemetry
      const diagRes = await client.query(`
        SELECT data FROM importer_diagnostic_telemetry
        WHERE session_id = $1
        ORDER BY created_at DESC LIMIT 1
      `, [sessionId]);
      const diagData = diagRes.rows[0]?.data || {};
      const activeWorkersAvg = diagData.activeWorkers?.avg || importingNow;
      const activeWorkersPeak = diagData.activeWorkers?.peak || importingNow;
      const dbPoolWaitAvg = diagData.yugabyteDbPool?.waitAvgMs || 0;
      const dbPoolWaitP95 = diagData.yugabyteDbPool?.waitP95Ms || 0;

      const sample = {
        idx: sampleIdx,
        timestamp: new Date().toISOString(),
        elapsedSec,
        totalConns,
        directConns,
        hyperdriveConns,
        idleInTx,
        cpuPercent: parseFloat(cpu.toFixed(2)),
        discloudCpu: discloud.cpu,
        discloudRam: discloud.ram,
        importingNow,
        queuedNow,
        completedChapters,
        totalPages,
        mediaCount,
        totalBytesMb: parseFloat(totalBytesMb),
        pagesPerMin: parseFloat(pagesPerMin),
        capPerMin: parseFloat(capPerMin),
        mbPerMin: parseFloat(mbPerMin),
        activeWorkersAvg,
        activeWorkersPeak,
        dbPoolWaitAvg,
        dbPoolWaitP95,
        homeTtfb: homeRes.ttfb,
        readerTtfb: readerRes.ttfb,
        mediaTtfb: mediaProbeRes.ttfb
      };
      samples.push(sample);

      console.log(
        `[#${sampleIdx} ${elapsedSec}s/${Math.round(durationMs/1000)}s] ` +
        `Active: ${importingNow}/${configuredWorkers} (${queuedNow} queued) | ` +
        `Done: ${completedChapters} cap (${capPerMin} c/m, ${pagesPerMin} p/m, ${mbPerMin} MB/m) | ` +
        `DB Pool Wait: avg ${dbPoolWaitAvg}ms (p95: ${dbPoolWaitP95}ms) | ` +
        `YB CPU: ${sample.cpuPercent.toFixed(1)}% | YSQL: ${sample.totalConns}/${maxConnections} | ` +
        `Discloud: ${sample.discloudCpu}% CPU, ${sample.discloudRam}MB | Site: H:${sample.homeTtfb}ms R:${sample.readerTtfb}ms M:${sample.mediaTtfb}ms`
      );

      // TRIPWIRES
      if (totalConns >= 11) {
        console.warn(`⚠️ TRIPWIRE ALERT: YSQL connections touched ${totalConns}/${maxConnections}`);
      }
      if (totalConns >= 12) {
        emergencyTriggered = true;
        emergencyReason = `CRITICAL TRIPWIRE: YSQL connections reached ${totalConns}/${maxConnections} (Limit 12)`;
        console.error(`🚨 ${emergencyReason} — INITIATING EMERGENCY STOP`);
        break;
      }

      const elapsed = Date.now() - t0;
      const waitTime = Math.max(100, (sampleIntervalSec * 1000) - elapsed);
      await new Promise(r => setTimeout(r, waitTime));
    }
  } finally {
    console.log('\n======================================================================');
    console.log(`🛑 RAMP DOWN INITIATED FOR STAGE: ${stageName.toUpperCase()}`);
    console.log('======================================================================');

    // 1. Close publication safety barrier
    console.log('[Rampdown 1] Closing Publication Safety Barrier...');
    await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");

    // 2. Pause chapter sources
    console.log('[Rampdown 2] Pausing chapter sources...');
    await client.query(`
      UPDATE importer_sources 
      SET status = 'PAUSED', updated_at = NOW() 
      WHERE id = ANY($1)
    `, [ACTIVE_SOURCES]);

    // 3. Await active in-flight jobs to drain
    console.log('[Rampdown 3] Waiting for in-flight jobs to drain (max 90s)...');
    let activeCh = 1;
    let pollAttempts = 0;
    while (activeCh > 0 && pollAttempts < 45) {
      pollAttempts++;
      const qRes = await client.query(`
        SELECT count(*) as count 
        FROM importer_queue 
        WHERE task_type = 'IMPORT_CHAPTER' AND status IN ('IMPORTING', 'RUNNING')
      `);
      activeCh = parseInt(qRes.rows[0].count, 10);
      if (activeCh > 0) {
        console.log(`  [Drain] In-flight jobs remaining: ${activeCh}... waiting 2s`);
        await new Promise(r => setTimeout(r, 2000));
      }
    }
    console.log(`✅ In-flight jobs drained: ${activeCh} active.`);

    // 4. Mark diagnostic session complete
    await client.query("UPDATE settings SET value = 'IDLE' WHERE key = 'active_diagnostic_session'");

    // 5. Fetch final diagnostic report
    console.log('[Telemetry] Pulling full telemetry from Yugabyte...');
    const finalDiagRes = await client.query(`
      SELECT data FROM importer_diagnostic_telemetry
      WHERE session_id = $1
      ORDER BY created_at DESC LIMIT 1
    `, [sessionId]);
    const diag = finalDiagRes.rows[0]?.data || {};

    // 6. Fetch exact completed jobs for this session
    const finalCompRes = await client.query(`
      SELECT source, count(*) as count,
             COALESCE(sum(progress_current), 0) as pages
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER'
        AND status = 'COMPLETED'
        AND updated_at >= $1
      GROUP BY source
    `, [benchmarkStartTimeIso]);

    const finalMediaRes = await client.query(`
      SELECT count(*) as count,
             COALESCE(sum(bytes), 0) as total_bytes
      FROM media
      WHERE created_at >= $1
    `, [benchmarkStartTimeIso]);

    // Query job metrics table
    const jobMetricsRes = await client.query(`
      SELECT source, chapter_number, page_count, total_bytes, duration_ms, download_ms, upload_ms, db_ms, status, created_at
      FROM importer_job_metrics
      WHERE created_at >= $1
      ORDER BY created_at ASC
    `, [benchmarkStartTimeIso]);

    const totalDurationSec = Math.round((Date.now() - startTime) / 1000);
    const totalDurationMin = totalDurationSec / 60;
    const finalCompletedChapters = finalCompRes.rows.reduce((a, b) => a + parseInt(b.count, 10), 0);
    const finalTotalPages = finalCompRes.rows.reduce((a, b) => a + parseInt(b.pages, 10), 0);
    const finalTotalBytes = parseInt(finalMediaRes.rows[0]?.total_bytes || 0, 10);
    const finalTotalBytesMb = (finalTotalBytes / (1024 * 1024)).toFixed(1);

    const capMinAvg = (finalCompletedChapters / totalDurationMin).toFixed(2);
    const pagesMinAvg = (finalTotalPages / totalDurationMin).toFixed(1);
    const mbMinAvg = (parseFloat(finalTotalBytesMb) / totalDurationMin).toFixed(2);

    let jobDurations = jobMetricsRes.rows.map(r => r.duration_ms).filter(Boolean);
    if (jobDurations.length === 0 && diag.slowestChapters && diag.slowestChapters.length > 0) {
      jobDurations = diag.slowestChapters.map(c => c.totalDurationMs).filter(Boolean);
    }
    const avgJobDuration = avg(jobDurations);
    const p50JobDuration = percentile(jobDurations, 0.50);
    const p95JobDuration = percentile(jobDurations, 0.95);

    const siteErrorRate = totalSiteProbes > 0 ? ((siteErrors / totalSiteProbes) * 100).toFixed(2) : '0.00';

    const fullSummary = {
      stageName,
      sessionId,
      configuredWorkers,
      actualDurationSeconds: totalDurationSec,
      benchmarkStartTimeIso,
      benchmarkEndTimeIso: new Date().toISOString(),
      throughput: {
        totalChaptersCompleted: finalCompletedChapters,
        totalPages: finalTotalPages,
        totalBytesMb: parseFloat(finalTotalBytesMb),
        capPerMin: parseFloat(capMinAvg),
        pagesPerMin: parseFloat(pagesMinAvg),
        mbPerMin: parseFloat(mbMinAvg),
        avgJobDurationMs: avgJobDuration,
        p50JobDurationMs: p50JobDuration,
        p95JobDurationMs: p95JobDuration,
      },
      effectiveWorkers: {
        avg: avg(effectiveActiveSamples),
        p50: percentile(effectiveActiveSamples, 0.50),
        p95: percentile(effectiveActiveSamples, 0.95),
        peak: Math.max(...effectiveActiveSamples, 0),
      },
      siteLatency: {
        home: {
          p50: percentile(homeLatencies, 0.50),
          p90: percentile(homeLatencies, 0.90),
          p95: percentile(homeLatencies, 0.95),
          p99: percentile(homeLatencies, 0.99),
          max: Math.max(...homeLatencies, 0),
          targetMet: percentile(homeLatencies, 0.95) <= 250,
        },
        reader: {
          p50: percentile(readerLatencies, 0.50),
          p90: percentile(readerLatencies, 0.90),
          p95: percentile(readerLatencies, 0.95),
          p99: percentile(readerLatencies, 0.99),
          max: Math.max(...readerLatencies, 0),
          targetMet: percentile(readerLatencies, 0.95) <= 150,
        },
        media: {
          p50: percentile(mediaLatencies, 0.50),
          p90: percentile(mediaLatencies, 0.90),
          p95: percentile(mediaLatencies, 0.95),
          p99: percentile(mediaLatencies, 0.99),
          max: Math.max(...mediaLatencies, 0),
          targetMet: percentile(mediaLatencies, 0.95) <= 120,
        },
        httpErrorRate: `${siteErrorRate}%`,
        errors: siteErrors,
        totalProbes: totalSiteProbes,
      },
      ysql: {
        connectionsAvg: avg(ysqlConnSamples),
        connectionsP95: percentile(ysqlConnSamples, 0.95),
        connectionsPeak: Math.max(...ysqlConnSamples, 0),
        maxConnections,
        tripwireTriggered: emergencyTriggered,
        tripwireReason: emergencyReason,
      },
      yugabyteDb: {
        cpuAvg: avg(ybCpuSamples),
        cpuP95: percentile(ybCpuSamples, 0.95),
        cpuPeak: Math.max(...ybCpuSamples, 0),
        dbPoolWaitAvgMs: diag.yugabyteDbPool?.waitAvgMs || 0,
        dbPoolWaitP95Ms: diag.yugabyteDbPool?.waitP95Ms || 0,
        dbPoolWaitMaxMs: diag.yugabyteDbPool?.waitMaxMs || 0,
        queuedWaitingAvg: diag.yugabyteDbPool?.queuedWaitingAvg || 0,
        queuedWaitingPeak: diag.yugabyteDbPool?.queuedWaitingPeak || 0,
      },
      telegram: {
        activeUploadsAvg: diag.telegramStorage?.activeUploadsAvg || 0,
        activeUploadsP95: diag.telegramStorage?.activeUploadsP95 || 0,
        activeUploadsPeak: diag.telegramStorage?.activeUploadsPeak || 0,
        pageUploadAvgMs: diag.telegramStorage?.pageUploadDurationAvg || 0,
        pageUploadP95Ms: diag.telegramStorage?.pageUploadDurationP95 || 0,
        semaphoreWaitAvgMs: diag.telegramStorage?.semaphoreWaitAvgMs || 0,
        semaphoreWaitP95Ms: diag.telegramStorage?.semaphoreWaitP95Ms || 0,
        rateLimit429: diag.telegramStorage?.rateLimit429Count || 0,
        floodWait: diag.telegramStorage?.floodWaitCount || 0,
      },
      discloud: {
        cpuAvg: avg(discloudCpuSamples),
        cpuP95: percentile(discloudCpuSamples, 0.95),
        cpuPeak: Math.max(...discloudCpuSamples, 0),
        ramAvgMb: avg(discloudRamSamples),
        ramPeakMb: Math.max(...discloudRamSamples, 0),
        eventLoopLagAvgMs: diag.eventLoopAndNode?.eventLoopLagAvg || 0,
        eventLoopLagP95Ms: diag.eventLoopAndNode?.eventLoopLagP95 || 0,
        eventLoopLagMaxMs: diag.eventLoopAndNode?.eventLoopLagMax || 0,
        eventLoopUtilizationAvg: diag.eventLoopAndNode?.eventLoopUtilizationAvg || 0,
      },
      sources: {
        completedBySource: finalCompRes.rows,
        telemetryBySource: diag.sourceDistribution || {},
      },
      samples,
      telemetry: diag,
    };

    const outPath = `/home/awerkori/.Projects/project-nox-importer/benchmark_${stageName}_result.json`;
    fs.writeFileSync(outPath, JSON.stringify(fullSummary, null, 2));

    console.log('\n======================================================================');
    console.log(`📊 RESULTS SUMMARY FOR STAGE: ${stageName.toUpperCase()}`);
    console.log('======================================================================');
    console.log(`Workers Configured: ${configuredWorkers} | Effective Active Avg: ${fullSummary.effectiveWorkers.avg} (Peak: ${fullSummary.effectiveWorkers.peak})`);
    console.log(`Throughput: ${finalCompletedChapters} chapters in ${totalDurationMin.toFixed(1)}m => ${capMinAvg} CAP/MIN | ${pagesMinAvg} PAGES/MIN | ${mbMinAvg} MB/MIN`);
    console.log(`Job Duration: Avg ${avgJobDuration}ms | P50 ${p50JobDuration}ms | P95 ${p95JobDuration}ms`);
    console.log(`Site TTFB: Home p95=${fullSummary.siteLatency.home.p95}ms | Reader p95=${fullSummary.siteLatency.reader.p95}ms | Media p95=${fullSummary.siteLatency.media.p95}ms | Error Rate: ${siteErrorRate}%`);
    console.log(`YSQL Connections: avg ${fullSummary.ysql.connectionsAvg} | p95 ${fullSummary.ysql.connectionsP95} | peak ${fullSummary.ysql.connectionsPeak}/${maxConnections}`);
    console.log(`DB Pool Wait: avg ${fullSummary.yugabyteDb.dbPoolWaitAvgMs}ms | p95 ${fullSummary.yugabyteDb.dbPoolWaitP95Ms}ms | peak wait ${fullSummary.yugabyteDb.dbPoolWaitMaxMs}ms`);
    console.log(`Telegram Storage: page upload avg ${fullSummary.telegram.pageUploadAvgMs}ms | semaphore wait avg ${fullSummary.telegram.semaphoreWaitAvgMs}ms | 429: ${fullSummary.telegram.rateLimit429} | FloodWait: ${fullSummary.telegram.floodWait}`);
    console.log(`Discloud Node: CPU avg ${fullSummary.discloud.cpuAvg}% (peak ${fullSummary.discloud.cpuPeak}%) | RAM avg ${fullSummary.discloud.ramAvgMb}MB (peak ${fullSummary.discloud.ramPeakMb}MB) | Lag p95: ${fullSummary.discloud.eventLoopLagP95Ms}ms`);
    console.log(`Saved full dataset to: ${outPath}`);
    console.log('======================================================================\n');

    await client.end();
  }
}

main().catch(err => {
  console.error('Fatal benchmark execution error:', err);
  process.exit(1);
});
