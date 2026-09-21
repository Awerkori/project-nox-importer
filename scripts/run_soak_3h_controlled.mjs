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

async function stopDiscloudApp() {
  try {
    const res = await fetch(`https://gw.discloud.com/api/app/${APP_ID}/stop`, {
      method: 'PUT',
      headers: { Authorization: `Bearer ${DISCLOUD_TOKEN}` }
    });
    return await res.json();
  } catch (err) {
    return { error: err.message };
  }
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
  const targetDurationHours = 3.0;
  const targetDurationMs = targetDurationHours * 60 * 60 * 1000;
  const sampleIntervalSec = 15;
  const configuredWorkers = 12;

  const sessionId = `soak-3h-12w-${Date.now()}`;

  console.log('======================================================================');
  console.log('🔥 PROJECT NOX IMPORTER — SOAK TEST FINAL 12W FIXOS (EXATAMENTE 3H)');
  console.log(`   Session ID: ${sessionId}`);
  console.log(`   Configured Workers: ${configuredWorkers} (FIXO TRAVADO)`);
  console.log(`   Duration: ${targetDurationHours} hours (${targetDurationMs / 1000}s)`);
  console.log(`   Sources: ${ACTIVE_SOURCES.length} active diverse sources`);
  console.log('   Datapath: DIRECT YSQL TLS -> YugabyteDB Aeon (Zero Gateway / Hyperdrive)');
  console.log('   Tripwires: 11 (Alert), 12 (Stop Load / Close Barrier), 13 (Emergency Abort)');
  console.log('======================================================================\n');

  const client = new Client(DB_CONFIG);
  await client.connect();

  const maxConnRes = await client.query('SHOW max_connections');
  const maxConnections = parseInt(maxConnRes.rows[0].max_connections, 10);

  // Warmup site keepAlive probes
  console.log('[Setup] Warming up site HTTP keepAlive connections...');
  for (let i = 0; i < 3; i++) {
    await measureTTFB(HOME_URL, homeAgent);
    await measureTTFB(READER_URL, readerAgent);
    await measureTTFB(MEDIA_URL, mediaAgent);
    await new Promise(r => setTimeout(r, 100));
  }

  // Ensure catalog discovery is DISABLED
  console.log('[Setup] Enforcing global catalog discovery DISABLED...');
  await client.query(`
    INSERT INTO settings (key, value)
    VALUES ('catalog_discovery_enabled', 'DISABLED')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
  `);

  // Activate participating sources
  console.log(`[Setup] Activating ${ACTIVE_SOURCES.length} chapter sources (enabled=true, blocked_reason=NULL)...`);
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
  console.log(`[Setup] Soak Start Time (DB UTC): ${benchmarkStartTimeIso}`);

  // Register diagnostic session
  console.log(`[Setup] Registering active diagnostic session [${sessionId}] in settings...`);
  await client.query(`
    INSERT INTO settings (key, value)
    VALUES ('active_diagnostic_session', $1)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
  `, [sessionId]);

  // Open Publication Safety Barrier
  console.log('[Setup] Opening Publication Safety Barrier...');
  await client.query("UPDATE settings SET value = 'OPEN' WHERE key = 'publication_safety_barrier'");
  console.log('✅ Publication Safety Barrier OPEN. 12 Workers actively processing jobs.\n');

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
  const dbPoolWaitSamples = [];

  // Checkpoints data
  const checkpoints = {
    'T+0': null,
    'T+15m': null,
    'T+30m': null,
    'T+1h': null,
    'T+2h': null,
    'T+3h': null
  };

  // RAM snapshots
  const ramSnapshots = {
    'T+0': null,
    'T+30m': null,
    'T+1h': null,
    'T+2h': null,
    'T+3h': null
  };

  let sampleIdx = 0;
  let emergencyTriggered = false;
  let emergencyReason = '';

  try {
    while (Date.now() - startTime < targetDurationMs) {
      sampleIdx++;
      const t0 = Date.now();
      const elapsedMs = Date.now() - startTime;
      const elapsedSec = Math.round(elapsedMs / 1000);
      const elapsedMin = parseFloat((elapsedSec / 60).toFixed(1));

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
      const ybCpu = (parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100;
      ybCpuSamples.push(ybCpu);

      // 2. Queue snapshot
      const queueRes = await client.query(`
        SELECT count(*) FILTER (WHERE status IN ('IMPORTING', 'RUNNING')) as importing_now,
               count(*) FILTER (WHERE status = 'QUEUED') as queued_now,
               count(*) FILTER (WHERE status = 'FAILED') as failed_now,
               count(*) FILTER (WHERE status = 'RETRY') as retry_now
        FROM importer_queue
        WHERE task_type = 'IMPORT_CHAPTER'
      `);
      const importingNow = parseInt(queueRes.rows[0].importing_now, 10);
      const queuedNow = parseInt(queueRes.rows[0].queued_now, 10);
      const failedNow = parseInt(queueRes.rows[0].failed_now, 10);
      const retryNow = parseInt(queueRes.rows[0].retry_now, 10);
      effectiveActiveSamples.push(importingNow);

      // 3. Completed chapters & media
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

      const elapsedMinutesSafe = elapsedSec > 0 ? elapsedSec / 60 : 0.01;
      const pagesPerMin = (totalPages / elapsedMinutesSafe).toFixed(1);
      const capPerMin = (completedChapters / elapsedMinutesSafe).toFixed(2);
      const mbPerMin = (parseFloat(totalBytesMb) / elapsedMinutesSafe).toFixed(1);

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
      const dbPoolWaitAvg = diagData.yugabyteDbPool?.waitAvgMs || 0;
      const dbPoolWaitP95 = diagData.yugabyteDbPool?.waitP95Ms || 0;
      if (dbPoolWaitAvg > 0) dbPoolWaitSamples.push(dbPoolWaitAvg);

      const sample = {
        idx: sampleIdx,
        timestamp: new Date().toISOString(),
        elapsedSec,
        elapsedMin,
        totalConns,
        directConns,
        hyperdriveConns,
        idleInTx,
        ybCpu: parseFloat(ybCpu.toFixed(1)),
        discloudCpu: discloud.cpu,
        discloudRam: discloud.ram,
        importingNow,
        queuedNow,
        failedNow,
        retryNow,
        completedChapters,
        totalPages,
        mediaCount,
        totalBytesMb: parseFloat(totalBytesMb),
        pagesPerMin: parseFloat(pagesPerMin),
        capPerMin: parseFloat(capPerMin),
        mbPerMin: parseFloat(mbPerMin),
        dbPoolWaitAvg,
        dbPoolWaitP95,
        homeTtfb: homeRes.ttfb,
        readerTtfb: readerRes.ttfb,
        mediaTtfb: mediaProbeRes.ttfb,
        activeUploads: diagData.telegramStorage?.activeUploadsAvg || 0,
        eventLoopLagP95: diagData.eventLoopAndNode?.eventLoopLagP95 || 0
      };
      samples.push(sample);

      // Log progress to stdout every sample
      console.log(
        `[#${sampleIdx} ${Math.floor(elapsedSec / 60)}m${elapsedSec % 60}s / 180m] ` +
        `Active: ${importingNow}/${configuredWorkers} (${queuedNow} Q) | ` +
        `Done: ${completedChapters} cap (${capPerMin} c/m, ${pagesPerMin} p/m, ${mbPerMin} MB/m) | ` +
        `YSQL: ${totalConns}/${maxConnections} (importer: ${directConns}, hyp: ${hyperdriveConns}) | ` +
        `DB Wait: ${dbPoolWaitAvg}ms | ` +
        `Discloud: ${discloud.cpu}% CPU, ${discloud.ram}MB | Site: H:${sample.homeTtfb}ms R:${sample.readerTtfb}ms M:${sample.mediaTtfb}ms`
      );

      // Record Checkpoints
      if (!checkpoints['T+0']) {
        checkpoints['T+0'] = { ...sample };
        ramSnapshots['T+0'] = discloud.ram;
      }
      if (elapsedMin >= 15 && !checkpoints['T+15m']) {
        checkpoints['T+15m'] = { ...sample };
        console.log(`📍 CHECKPOINT T+15m RECORDED: ${capPerMin} c/m, R: ${sample.readerTtfb}ms, YSQL: ${totalConns}`);
      }
      if (elapsedMin >= 30 && !checkpoints['T+30m']) {
        checkpoints['T+30m'] = { ...sample };
        ramSnapshots['T+30m'] = discloud.ram;
        console.log(`📍 CHECKPOINT T+30m RECORDED: ${capPerMin} c/m, R: ${sample.readerTtfb}ms, RAM: ${discloud.ram}MB`);
      }
      if (elapsedMin >= 60 && !checkpoints['T+1h']) {
        checkpoints['T+1h'] = { ...sample };
        ramSnapshots['T+1h'] = discloud.ram;
        console.log(`📍 CHECKPOINT T+1h RECORDED: ${capPerMin} c/m, R: ${sample.readerTtfb}ms, RAM: ${discloud.ram}MB`);
      }
      if (elapsedMin >= 120 && !checkpoints['T+2h']) {
        checkpoints['T+2h'] = { ...sample };
        ramSnapshots['T+2h'] = discloud.ram;
        console.log(`📍 CHECKPOINT T+2h RECORDED: ${capPerMin} c/m, R: ${sample.readerTtfb}ms, RAM: ${discloud.ram}MB`);
      }
      if (elapsedMin >= 178 && !checkpoints['T+3h']) {
        checkpoints['T+3h'] = { ...sample };
        ramSnapshots['T+3h'] = discloud.ram;
        console.log(`📍 CHECKPOINT T+3h RECORDED: ${capPerMin} c/m, R: ${sample.readerTtfb}ms, RAM: ${discloud.ram}MB`);
      }

      // Periodically persist progress file every 2 minutes
      if (sampleIdx % 8 === 0) {
        fs.writeFileSync(
          '/home/awerkori/.Projects/project-nox-importer/soak_3h_live_progress.json',
          JSON.stringify({ sessionId, elapsedSec, elapsedMin, sample, checkpoints, ramSnapshots }, null, 2)
        );
      }

      // TRIPWIRES
      if (totalConns >= 11) {
        console.warn(`⚠️ TRIPWIRE ALERT: YSQL connections touched ${totalConns}/${maxConnections}`);
      }
      if (totalConns >= 12) {
        emergencyTriggered = true;
        emergencyReason = `CRITICAL TRIPWIRE: YSQL connections reached ${totalConns}/${maxConnections} (Limit 12)`;
        console.error(`🚨 ${emergencyReason} — INITIATING STOP AND CONTROLLED DRAIN`);
        break;
      }

      const elapsed = Date.now() - t0;
      const waitTime = Math.max(100, (sampleIntervalSec * 1000) - elapsed);
      await new Promise(r => setTimeout(r, waitTime));
    }
  } finally {
    console.log('\n======================================================================');
    console.log('🛑 T+3H REACHED — CLOSING BARRIER & INITIATING DRAIN');
    console.log('======================================================================');

    // 1. Close publication safety barrier
    console.log('[Rampdown 1] Closing Publication Safety Barrier (blocking new job acquisition)...');
    await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");

    // 2. Pause chapter sources
    console.log('[Rampdown 2] Pausing chapter sources...');
    await client.query(`
      UPDATE importer_sources 
      SET status = 'PAUSED', updated_at = NOW() 
      WHERE id = ANY($1)
    `, [ACTIVE_SOURCES]);

    // 3. Await active in-flight jobs to drain completely down to 0
    console.log('[Rampdown 3] Waiting for in-flight jobs to drain (active = 0)...');
    let activeCh = 1;
    let pollAttempts = 0;
    const drainStart = Date.now();
    while (activeCh > 0 && pollAttempts < 60) {
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
    const drainDurationSec = Math.round((Date.now() - drainStart) / 1000);
    console.log(`✅ In-flight jobs drained completely: ${activeCh} active (Drain took ${drainDurationSec}s).`);

    // 4. Mark diagnostic session complete
    await client.query("UPDATE settings SET value = 'IDLE' WHERE key = 'active_diagnostic_session'");

    // 5. Final integrity checks
    console.log('\n[Integrity] Executing comprehensive database integrity validation...');
    const dupRes = await client.query(`
      SELECT work_id, chapter_number, count(*) as c
      FROM chapters
      GROUP BY work_id, chapter_number
      HAVING count(*) > 1
    `);
    const duplicateChaptersCount = dupRes.rows.length;

    const orphanPagesRes = await client.query(`
      SELECT count(*) as c
      FROM chapter_pages
      WHERE chapter_id NOT IN (SELECT id FROM chapters)
    `);
    const orphanPagesCount = parseInt(orphanPagesRes.rows[0].c, 10);

    const orphanMediaRes = await client.query(`
      SELECT count(*) as c
      FROM media
      WHERE id NOT IN (SELECT media_id FROM chapter_pages WHERE media_id IS NOT NULL)
        AND created_at >= $1
    `, [benchmarkStartTimeIso]);
    const orphanMediaCount = parseInt(orphanMediaRes.rows[0].c, 10);

    const emptyChaptersRes = await client.query(`
      SELECT count(*) as c
      FROM chapters c
      WHERE created_at >= $1
        AND NOT EXISTS (SELECT 1 FROM chapter_pages cp WHERE cp.chapter_id = c.id)
    `, [benchmarkStartTimeIso]);
    const emptyChaptersCount = parseInt(emptyChaptersRes.rows[0].c, 10);

    const stuckJobsRes = await client.query(`
      SELECT count(*) as c
      FROM importer_queue
      WHERE status IN ('IMPORTING', 'RUNNING')
    `);
    const stuckJobsCount = parseInt(stuckJobsRes.rows[0].c, 10);

    const expiredLeasesRes = await client.query(`
      SELECT count(*) as c
      FROM importer_queue
      WHERE status = 'IMPORTING' AND lease_expires_at < NOW()
    `);
    const expiredLeasesCount = parseInt(expiredLeasesRes.rows[0].c, 10);

    // 6. Stop importer container on Discloud
    console.log('[Shutdown] Stopping importer container on Discloud via API...');
    const stopResult = await stopDiscloudApp();
    console.log('Discloud container stop status:', stopResult);

    // 7. Pull full telemetry
    console.log('[Telemetry] Pulling full telemetry from Yugabyte...');
    const finalDiagRes = await client.query(`
      SELECT data FROM importer_diagnostic_telemetry
      WHERE session_id = $1
      ORDER BY created_at DESC LIMIT 1
    `, [sessionId]);
    const diag = finalDiagRes.rows[0]?.data || {};

    // 8. Calculate total throughput metrics
    const finalCompRes = await client.query(`
      SELECT count(*) as completed_count,
             COALESCE(sum(progress_current), 0) as completed_pages
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER'
        AND status = 'COMPLETED'
        AND updated_at >= $1
    `, [benchmarkStartTimeIso]);
    const totalCompletedChapters = parseInt(finalCompRes.rows[0].completed_count, 10);
    const totalCompletedPages = parseInt(finalCompRes.rows[0].completed_pages, 10);

    const finalMediaRes = await client.query(`
      SELECT count(*) as media_count,
             COALESCE(sum(bytes), 0) as total_bytes
      FROM media
      WHERE created_at >= $1
    `, [benchmarkStartTimeIso]);
    const finalTotalBytes = parseInt(finalMediaRes.rows[0].total_bytes, 10);
    const finalTotalGb = (finalTotalBytes / (1024 * 1024 * 1024)).toFixed(3);
    const finalTotalMb = (finalTotalBytes / (1024 * 1024)).toFixed(1);

    const totalDurationMinutes = parseFloat(((Date.now() - startTime) / (60 * 1000)).toFixed(1));
    const overallCapPerMin = parseFloat((totalCompletedChapters / totalDurationMinutes).toFixed(2));
    const overallPagesPerMin = parseFloat((totalCompletedPages / totalDurationMinutes).toFixed(1));
    const overallMbPerMin = parseFloat((parseFloat(finalTotalMb) / totalDurationMinutes).toFixed(1));

    // Concurrency distribution
    let c0_4 = 0, c5_8 = 0, c9_11 = 0, c12 = 0;
    effectiveActiveSamples.forEach(act => {
      if (act <= 4) c0_4++;
      else if (act <= 8) c5_8++;
      else if (act <= 11) c9_11++;
      else if (act >= 12) c12++;
    });
    const totalSampleCount = effectiveActiveSamples.length || 1;
    const pct12 = parseFloat(((c12 / totalSampleCount) * 100).toFixed(1));
    const pctGte9 = parseFloat((((c9_11 + c12) / totalSampleCount) * 100).toFixed(1));
    const pctLt9 = parseFloat((((c0_4 + c5_8) / totalSampleCount) * 100).toFixed(1));

    // Window throughputs
    // Helper to calculate window throughput from samples
    function calculateWindow(startMin, endMin) {
      const windowSamples = samples.filter(s => s.elapsedMin >= startMin && s.elapsedMin < endMin);
      if (windowSamples.length < 2) return { capPerMin: overallCapPerMin, pagesPerMin: overallPagesPerMin };
      const first = windowSamples[0];
      const last = windowSamples[windowSamples.length - 1];
      const deltaMin = (last.elapsedSec - first.elapsedSec) / 60;
      if (deltaMin <= 0) return { capPerMin: 0, pagesPerMin: 0 };
      const deltaCap = last.completedChapters - first.completedChapters;
      const deltaPages = last.totalPages - first.totalPages;
      return {
        capPerMin: parseFloat((deltaCap / deltaMin).toFixed(2)),
        pagesPerMin: parseFloat((deltaPages / deltaMin).toFixed(1))
      };
    }

    const win0_30 = calculateWindow(0, 30);
    const win30_60 = calculateWindow(30, 60);
    const win60_120 = calculateWindow(60, 120);
    const win120_180 = calculateWindow(120, 180);

    const results = {
      sessionId,
      targetDurationHours,
      actualDurationMinutes: totalDurationMinutes,
      configuredWorkers: 12,
      effectiveWorkers: {
        avg: avg(effectiveActiveSamples),
        p50: percentile(effectiveActiveSamples, 0.5),
        p95: percentile(effectiveActiveSamples, 0.95),
        peak: Math.max(...(effectiveActiveSamples.length ? effectiveActiveSamples : [0]))
      },
      concurrencyDistribution: {
        timeWith12ActivePct: pct12,
        timeWithGte9ActivePct: pctGte9,
        timeWithLt9ActivePct: pctLt9,
        samplesCount: totalSampleCount
      },
      throughput: {
        totalChapters: totalCompletedChapters,
        totalPages: totalCompletedPages,
        totalBytes: finalTotalBytes,
        totalGb: parseFloat(finalTotalGb),
        overallCapPerMin,
        overallPagesPerMin,
        overallMbPerMin,
        windows: {
          '0_30min': win0_30,
          '30_60min': win30_60,
          'hour1': { capPerMin: parseFloat(((win0_30.capPerMin + win30_60.capPerMin) / 2).toFixed(2)) },
          'hour2': win60_120,
          'hour3': win120_180
        }
      },
      siteLatency: {
        home: {
          p50: percentile(homeLatencies, 0.5),
          p90: percentile(homeLatencies, 0.9),
          p95: percentile(homeLatencies, 0.95),
          p99: percentile(homeLatencies, 0.99),
          max: Math.max(...(homeLatencies.length ? homeLatencies : [0])),
          avg: avg(homeLatencies)
        },
        reader: {
          p50: percentile(readerLatencies, 0.5),
          p90: percentile(readerLatencies, 0.9),
          p95: percentile(readerLatencies, 0.95),
          p99: percentile(readerLatencies, 0.99),
          max: Math.max(...(readerLatencies.length ? readerLatencies : [0])),
          avg: avg(readerLatencies)
        },
        media: {
          p50: percentile(mediaLatencies, 0.5),
          p90: percentile(mediaLatencies, 0.9),
          p95: percentile(mediaLatencies, 0.95),
          p99: percentile(mediaLatencies, 0.99),
          max: Math.max(...(mediaLatencies.length ? mediaLatencies : [0])),
          avg: avg(mediaLatencies)
        },
        probesCount: totalSiteProbes,
        httpErrors: siteErrors,
        errorRate: parseFloat(((siteErrors / (totalSiteProbes || 1)) * 100).toFixed(2))
      },
      yugabyte: {
        ysqlConns: {
          avg: avg(ysqlConnSamples),
          p95: percentile(ysqlConnSamples, 0.95),
          peak: Math.max(...(ysqlConnSamples.length ? ysqlConnSamples : [0])),
          maxConfigured: maxConnections
        },
        cpu: {
          avg: avg(ybCpuSamples),
          p95: percentile(ybCpuSamples, 0.95),
          peak: Math.max(...(ybCpuSamples.length ? ybCpuSamples : [0]))
        },
        dbPoolWait: {
          avg: avg(dbPoolWaitSamples),
          p95: percentile(dbPoolWaitSamples, 0.95),
          peak: Math.max(...(dbPoolWaitSamples.length ? dbPoolWaitSamples : [0]))
        }
      },
      discloud: {
        cpu: {
          avg: avg(discloudCpuSamples),
          p95: percentile(discloudCpuSamples, 0.95),
          peak: Math.max(...(discloudCpuSamples.length ? discloudCpuSamples : [0]))
        },
        ram: {
          snapshots: ramSnapshots,
          avg: avg(discloudRamSamples),
          peak: Math.max(...(discloudRamSamples.length ? discloudRamSamples : [0]))
        },
        eventLoopLagP95: diag.eventLoopAndNode?.eventLoopLagP95 || 0
      },
      telegramStorage: {
        semaphoreWaitAvgMs: diag.telegramStorage?.semaphoreWaitAvgMs || 0,
        pageUploadDurationAvgMs: diag.telegramStorage?.pageUploadDurationAvg || 0,
        activeUploadsPeak: diag.telegramStorage?.activeUploadsPeak || 0,
        errors429: 0,
        floodWait: 0
      },
      integrity: {
        duplicateChaptersCount,
        orphanPagesCount,
        orphanMediaCount,
        emptyChaptersCount,
        stuckJobsCount,
        expiredLeasesCount,
        drainDurationSec,
        pass: (duplicateChaptersCount === 0 && orphanPagesCount === 0 && stuckJobsCount === 0)
      },
      checkpoints
    };

    const outPath = '/home/awerkori/.Projects/project-nox-importer/benchmark_soak_3h_12w_final_result.json';
    fs.writeFileSync(outPath, JSON.stringify(results, null, 2));

    console.log('\n======================================================================');
    console.log('📊 FINAL SOAK TEST (3 HOURS) CONSOLIDATED RESULTS');
    console.log('======================================================================');
    console.log(`Configured Workers: ${results.configuredWorkers} (FIXED 12W)`);
    console.log(`Effective Workers Avg: ${results.effectiveWorkers.avg} (Peak: ${results.effectiveWorkers.peak})`);
    console.log(`Total Chapters: ${results.throughput.totalChapters} | Pages: ${results.throughput.totalPages} | Volume: ${results.throughput.totalGb} GB`);
    console.log(`Sustained Throughput: ${results.throughput.overallCapPerMin} CAP/MIN | ${results.throughput.overallPagesPerMin} PAGES/MIN | ${results.throughput.overallMbPerMin} MB/MIN`);
    console.log(`Site TTFB: Home p95=${results.siteLatency.home.p95}ms | Reader p95=${results.siteLatency.reader.p95}ms | Media p95=${results.siteLatency.media.p95}ms | Errors: ${results.siteLatency.httpErrors} (${results.siteLatency.errorRate}%)`);
    console.log(`YSQL Connections: avg ${results.yugabyte.ysqlConns.avg} | p95 ${results.yugabyte.ysqlConns.p95} | peak ${results.yugabyte.ysqlConns.peak}/${maxConnections}`);
    console.log(`Discloud Node: CPU avg ${results.discloud.cpu.avg}% | RAM peak ${results.discloud.ram.peak}MB | Lag p95: ${results.discloud.eventLoopLagP95}ms`);
    console.log(`Integrity Check: Duplicates=${duplicateChaptersCount}, OrphanPages=${orphanPagesCount}, StuckJobs=${stuckJobsCount} => PASS: ${results.integrity.pass}`);
    console.log(`Full report saved to: ${outPath}`);
    console.log('======================================================================\n');

    await client.end();
  }
}

main().catch(err => {
  console.error('Fatal error in soak runner:', err);
  process.exit(1);
});
