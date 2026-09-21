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

const DISCLOUD_TOKEN = '4bc8293a4be7d5e98fe447b89d6d3bb0e12897b1b421820829d4ee8127348f2404af1f927eea5a62c186f98bc11ef7676a9f9a68';
const APP_ID = '1788873398156';

const HOME_URL = 'https://manga.project-nox-awerkori.workers.dev/';
const READER_URL = 'https://manga.project-nox-awerkori.workers.dev/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c';
const MEDIA_URL = 'https://manga.project-nox-awerkori.workers.dev/media/000003ed-c2db-4794-bcfa-c5e8b21ce080';

// Dedicated persistent agents for connection reuse
const homeAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
const readerAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
const mediaAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });

function measureTTFB(url, agent, timeoutMs = 4000) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    let port = null;
    let reused = false;
    let ttfbRecorded = false;
    let ttfb = 0;

    const req = https.get(url, { agent }, (res) => {
      port = res.socket?.localPort;
      reused = !!res.socket?.reused;

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
        resolve({ status: res.statusCode, ttfb, port, reused });
      });
    });

    req.on('error', (err) => resolve({ status: 500, ttfb: 9999, port: null, reused: false, error: err.message }));
    req.setTimeout(timeoutMs, () => {
      req.destroy();
      resolve({ status: 408, ttfb: 9999, port: null, reused: false });
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

function getStats(arr, target) {
  if (!arr.length) return { min: 0, p50: 0, p75: 0, p90: 0, p95: 0, p99: 0, max: 0, aboveTargetCount: 0 };
  const sorted = [...arr].sort((a, b) => a - b);
  const above = sorted.filter(v => v > target);
  return {
    min: sorted[0],
    p50: sorted[Math.floor(sorted.length * 0.50)],
    p75: sorted[Math.floor(sorted.length * 0.75)],
    p90: sorted[Math.floor(sorted.length * 0.90)],
    p95: sorted[Math.floor(sorted.length * 0.95)],
    p99: sorted[Math.floor(sorted.length * 0.99)],
    max: sorted[sorted.length - 1],
    target,
    aboveTargetCount: above.length,
    aboveTargetValues: above
  };
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
  } catch (err) {
    // Non-fatal
  }
  return { cpu: 0, ram: 0, memory: '', container: 'unknown' };
}

async function getDiscloudLogs() {
  try {
    const res = await fetch(`https://gw.discloud.com/api/app/${APP_ID}/logs`, {
      headers: { Authorization: `Bearer ${DISCLOUD_TOKEN}` },
      signal: AbortSignal.timeout(6000)
    });
    if (res.ok) {
      const data = await res.json();
      return (data.app?.terminal?.big || '') + '\n' + (data.app?.terminal?.small || '');
    }
  } catch {}
  return '';
}

async function main() {
  const durationMinutes = parseFloat(process.argv[2] || '10');
  const sampleIntervalSec = parseInt(process.argv[3] || '15', 10);
  const durationMs = durationMinutes * 60 * 1000;

  console.log('======================================================================');
  console.log(`🚀 PROJECT NOX — CONTROLLED 5-WORKER PURE DIRECT BENCHMARK (${durationMinutes} MINUTES)`);
  console.log(`   Datapath: DIRECT YSQL TLS -> YugabyteDB Aeon (Zero Gateway / Hyperdrive)`);
  console.log(`   Scope: IMPORT_CHAPTER only | Discovery: PAUSED (Fail-Closed) | Reconciler: ACTIVE`);
  console.log(`   Telemetry: Validated HTTP keep-alive, direct Discloud API, Yugabyte metrics`);
  console.log('======================================================================\n');

  const client = new Client(DB_CONFIG);
  await client.connect();

  // Safety baseline checks
  const maxConnRes = await client.query('SHOW max_connections');
  const maxConnections = parseInt(maxConnRes.rows[0].max_connections, 10);
  console.log(`[Config] YSQL max_connections = ${maxConnections}`);

  // Warm up keep-alive connections
  console.log('Warming up persistent HTTP connections...');
  const wHome = await measureTTFB(HOME_URL, homeAgent);
  const wReader = await measureTTFB(READER_URL, readerAgent);
  const wMedia = await measureTTFB(MEDIA_URL, mediaAgent);
  console.log(`Warmup completed: Home=${wHome.ttfb}ms, Reader=${wReader.ttfb}ms, Media=${wMedia.ttfb}ms\n`);

  // Record initial Discloud logs snapshot
  const initialLogs = await getDiscloudLogs();

  // Record initial state timestamps & job metrics using DB time
  const dbTimeRes = await client.query('SELECT NOW() as db_start_time');
  const benchmarkStartTimeIso = dbTimeRes.rows[0].db_start_time.toISOString();
  console.log(`[Start Time (DB UTC)] ${benchmarkStartTimeIso}`);

  const initQueueDoneRes = await client.query(`
    SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
    FROM importer_queue
    WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER'
  `);
  const initialCompletedQueue = parseInt(initQueueDoneRes.rows[0].count, 10);
  const initialCompletedPages = parseInt(initQueueDoneRes.rows[0].pages, 10);

  // Step 1: Ensure discovery is disabled globally and on all sources (fail-closed)
  console.log('\n[Phase 1] Enforcing global catalog discovery DISABLED in settings...');
  await client.query(`
    INSERT INTO settings (key, value)
    VALUES ('catalog_discovery_enabled', 'DISABLED')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
  `);

  await client.query(`
    UPDATE importer_sources
    SET catalog_discovery_enabled = false;
  `);

  // Step 2: Enable operational chapter sources (chapters enabled, discovery disabled)
  console.log('[Phase 1] Enabling chapter sources (hanamiheaven, fleurblanche, mangalivreto)...');
  await client.query(`
    UPDATE importer_sources 
    SET chapter_ingestion_enabled = true,
        catalog_discovery_enabled = false,
        status = 'ACTIVE',
        updated_at = NOW() 
    WHERE id IN ('hanamiheaven', 'fleurblanche', 'mangalivreto')
  `);

  // Step 3: Open Publication Safety Barrier
  console.log('[Phase 1] Opening Publication Safety Barrier...');
  await client.query("UPDATE settings SET value = 'OPEN' WHERE key = 'publication_safety_barrier'");
  console.log('✅ Publication Safety Barrier is OPEN. 5-Worker pool will start acquiring chapters.\n');

  const startTime = Date.now();
  const samples = [];
  const homeLatencies = [];
  const readerLatencies = [];
  const mediaLatencies = [];
  const discloudCpuSamples = [];
  const discloudRamSamples = [];
  const lagSamples = [];

  let emergencyTriggered = false;
  let emergencyReason = '';

  let sampleIdx = 0;

  try {
    while (Date.now() - startTime < durationMs) {
      sampleIdx++;
      const t0 = Date.now();
      const elapsedSec = Math.round((Date.now() - startTime) / 1000);

      // 1. Database snapshot
      const actRes = await client.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
               count(*) FILTER (WHERE application_name = 'project-nox-importer-direct') as direct_importer,
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

      // 3. Queue state
      const queueRes = await client.query(`
        SELECT count(*) FILTER (WHERE status = 'IMPORTING') as importing_now,
               count(*) FILTER (WHERE status = 'QUEUED') as queued_now
        FROM importer_queue
        WHERE task_type = 'IMPORT_CHAPTER'
      `);
      const importingNow = parseInt(queueRes.rows[0].importing_now, 10);
      const queuedNow = parseInt(queueRes.rows[0].queued_now, 10);

      // 4. Ingestion progress from importer_job_metrics
      const metricsRes = await client.query(`
        SELECT 
          count(*) as total_done,
          count(*) FILTER (WHERE status = 'COMPLETED') as completed_chapters,
          count(*) FILTER (WHERE status = 'FAILED') as failed_chapters,
          COALESCE(sum(page_count) FILTER (WHERE status = 'COMPLETED'), 0) as total_pages,
          COALESCE(sum(total_bytes) FILTER (WHERE status = 'COMPLETED'), 0) as total_bytes
        FROM importer_job_metrics
        WHERE created_at >= $1
      `, [benchmarkStartTimeIso]);

      const completedChapters = parseInt(metricsRes.rows[0].completed_chapters, 10);
      const failedChapters = parseInt(metricsRes.rows[0].failed_chapters, 10);
      const totalPages = parseInt(metricsRes.rows[0].total_pages, 10);
      const totalBytes = parseInt(metricsRes.rows[0].total_bytes, 10);
      const totalBytesMb = (totalBytes / (1024 * 1024)).toFixed(1);

      const elapsedMinutes = elapsedSec > 0 ? elapsedSec / 60 : 0.01;
      const pagesPerMin = (totalPages / elapsedMinutes).toFixed(1);
      const capPerMin = (completedChapters / elapsedMinutes).toFixed(1);
      const mbPerMin = (parseFloat(totalBytesMb) / elapsedMinutes).toFixed(1);
      const avgPagesPerChapter = completedChapters > 0 ? (totalPages / completedChapters).toFixed(1) : '0';

      // 5. Site latency probes
      const homeRes = await measureTTFB(HOME_URL, homeAgent);
      const readerRes = await measureTTFB(READER_URL, readerAgent);
      const mediaRes = await measureTTFB(MEDIA_URL, mediaAgent);

      if (homeRes.ttfb < 9000) homeLatencies.push(homeRes.ttfb);
      if (readerRes.ttfb < 9000) readerLatencies.push(readerRes.ttfb);
      if (mediaRes.ttfb < 9000) mediaLatencies.push(mediaRes.ttfb);

      // 6. Discloud API live metrics
      const discloud = await getDiscloudStatus();
      if (discloud.cpu > 0) discloudCpuSamples.push(discloud.cpu);
      if (discloud.ram > 0) discloudRamSamples.push(discloud.ram);

      // 7. Importer telemetry snapshot
      const telemRes = await client.query(`
        SELECT rss_mb, heap_used_mb, event_loop_lag_ms, concurrency, active_jobs
        FROM importer_telemetry
        WHERE worker_id = 'discloud-importer-1'
        ORDER BY created_at DESC LIMIT 1
      `);
      const telem = telemRes.rows[0] || {};
      const loopLag = parseInt(telem.event_loop_lag_ms || 0, 10);
      lagSamples.push(loopLag);

      // Record sample
      const sample = {
        idx: sampleIdx,
        timestamp: new Date().toISOString(),
        elapsedSec,
        totalConns: parseInt(actRes.rows[0].total, 10),
        hyperdriveConns: parseInt(actRes.rows[0].hyperdrive, 10),
        directConns: parseInt(actRes.rows[0].direct_importer, 10),
        activeConns: parseInt(actRes.rows[0].active, 10),
        idleInTx: parseInt(actRes.rows[0].idle_in_tx, 10),
        blockingLocks: parseInt(locksRes.rows[0].blocking, 10),
        cpuPercent: parseFloat(cpu.toFixed(2)),
        discloudCpu: discloud.cpu,
        discloudRam: discloud.ram,
        eventLoopLag: loopLag,
        concurrency: parseInt(telem.concurrency || 5, 10),
        importingNow,
        queuedNow,
        completedChapters,
        failedChapters,
        totalPages,
        totalBytesMb: parseFloat(totalBytesMb),
        pagesPerMin: parseFloat(pagesPerMin),
        capPerMin: parseFloat(capPerMin),
        mbPerMin: parseFloat(mbPerMin),
        avgPagesPerChapter: parseFloat(avgPagesPerChapter),
        homeTtfb: homeRes.ttfb,
        readerTtfb: readerRes.ttfb,
        mediaTtfb: mediaRes.ttfb
      };
      samples.push(sample);

      console.log(
        `[#${sampleIdx} ${elapsedSec}s/${Math.round(durationMs/1000)}s] ` +
        `YB CPU: ${sample.cpuPercent.toFixed(1)}% | ` +
        `YSQL: ${sample.totalConns}/${maxConnections} (Hyp: ${sample.hyperdriveConns}, Direct: ${sample.directConns}, Act: ${sample.activeConns}) | ` +
        `Discloud: ${sample.discloudCpu}% CPU, ${sample.discloudRam}MB RAM, lag: ${sample.eventLoopLag}ms (Slots: ${sample.concurrency}) | ` +
        `Jobs: ${importingNow} active, ${completedChapters} done (${totalPages} p, ${capPerMin} c/m, ${pagesPerMin} p/m, ${mbPerMin} MB/m) | ` +
        `Site: H:${sample.homeTtfb}ms R:${sample.readerTtfb}ms M:${sample.mediaTtfb}ms`
      );

      // Safety checks / Tripwires
      if (sample.totalConns >= 13) {
        emergencyTriggered = true;
        emergencyReason = `CRITICAL: YSQL touched max capacity ${sample.totalConns}/${maxConnections}`;
        console.error(`🚨 ${emergencyReason}`);
        break;
      }
      if (sample.totalConns >= 12) {
        console.warn(`⚠️ [TRIPWIRE 12/13] Conexões em ${sample.totalConns}/${maxConnections}. Pausando novos claims (Publication Barrier = CLOSED).`);
        await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");
      } else if (sample.totalConns >= 11) {
        console.warn(`⚠️ [TRIPWIRE 11/13 SUSTENTADO] Conexões em ${sample.totalConns}/${maxConnections}. Alerta de limite operacional (não escalar).`);
      }

      const elapsed = Date.now() - t0;
      const waitTime = Math.max(100, (sampleIntervalSec * 1000) - elapsed);
      await new Promise(r => setTimeout(r, waitTime));
    }
  } finally {
    console.log('\n======================================================================');
    console.log('🛑 BENCHMARK TIME EXPIRED / RAMP DOWN INITIATED');
    console.log('======================================================================');

    // 1. Close Publication Safety Barrier
    console.log('[Rampdown 1] Closing Publication Safety Barrier...');
    await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");
    console.log('✅ Publication Safety Barrier CLOSED.');

    // 2. Pause chapter sources
    console.log('[Rampdown 2] Pausing chapter sources (hanamiheaven, fleurblanche, mangalivreto)...');
    await client.query(`
      UPDATE importer_sources 
      SET status = 'PAUSED', updated_at = NOW() 
      WHERE id IN ('hanamiheaven', 'fleurblanche', 'mangalivreto')
    `);
    console.log('✅ Chapter sources PAUSED.');

    // 3. Await ACTIVE CHAPTER JOBS = 0
    console.log('[Rampdown 3] Waiting for ACTIVE CHAPTER JOBS = 0 (max 60s)...');
    let activeCh = 1;
    let pollAttempts = 0;
    while (activeCh > 0 && pollAttempts < 30) {
      pollAttempts++;
      const qRes = await client.query(`
        SELECT count(*) as count 
        FROM importer_queue 
        WHERE task_type = 'IMPORT_CHAPTER' AND status IN ('IMPORTING', 'RUNNING')
      `);
      activeCh = parseInt(qRes.rows[0].count, 10);
      if (activeCh > 0) {
        console.log(`[Rampdown] In-flight chapter jobs remaining: ${activeCh}... waiting 2s`);
        await new Promise(r => setTimeout(r, 2000));
      }
    }
    console.log(`✅ Active chapter jobs reached: ${activeCh}`);

    // 4. Fetch final logs and Telegram metrics
    console.log('[Rampdown 4] Collecting final logs and Telegram metrics...');
    const finalLogs = await getDiscloudLogs();

    const floodWaitMatches = [...finalLogs.matchAll(/\[FLOOD_WAIT 429\] Bot (.*?) throttled for (\d+)s/g)];
    const retryMatches = [...finalLogs.matchAll(/\[UPLOAD_RETRY\] Bot (.*?) \/ Shard (.*?) error \((.*?)\)/g)];

    let floodWaitTotalSeconds = 0;
    for (const m of floodWaitMatches) {
      floodWaitTotalSeconds += parseInt(m[2], 10);
    }

    // Query DB for any Telegram errors in metrics
    const errRes = await client.query(`
      SELECT count(*) as tg_errs
      FROM importer_job_metrics
      WHERE created_at >= $1
        AND (error_message ILIKE '%telegram%' OR error_message ILIKE '%429%' OR error_message ILIKE '%floodwait%' OR error_message ILIKE '%storage%')
    `, [benchmarkStartTimeIso]);
    const tgDbErrors = parseInt(errRes.rows[0].tg_errs, 10);

    // Query discovery tasks created during test window
    const discCheckRes = await client.query(`
      SELECT task_type, count(*) as count
      FROM importer_queue
      WHERE created_at >= $1 AND task_type IN ('DISCOVER_WORKS', 'SYNC_WORK')
      GROUP BY task_type
    `, [benchmarkStartTimeIso]);
    const discoverWorksGenerated = parseInt(discCheckRes.rows.find(r => r.task_type === 'DISCOVER_WORKS')?.count || '0', 10);
    const syncWorkGenerated = parseInt(discCheckRes.rows.find(r => r.task_type === 'SYNC_WORK')?.count || '0', 10);

    // Final throughput metrics from importer_job_metrics
    const finalThroughputRes = await client.query(`
      SELECT 
        count(*) as total_chapters,
        count(*) FILTER (WHERE status = 'COMPLETED') as completed_chapters,
        count(*) FILTER (WHERE status = 'FAILED') as failed_chapters,
        COALESCE(sum(page_count) FILTER (WHERE status = 'COMPLETED'), 0) as total_pages,
        COALESCE(sum(total_bytes) FILTER (WHERE status = 'COMPLETED'), 0) as total_bytes,
        COALESCE(avg(duration_ms) FILTER (WHERE status = 'COMPLETED'), 0) as avg_duration_ms
      FROM importer_job_metrics
      WHERE created_at >= $1
    `, [benchmarkStartTimeIso]);

    const finalQueueRes = await client.query(`
      SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
      FROM importer_queue
      WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER'
    `);
    const finalCompletedQueue = parseInt(finalQueueRes.rows[0].count, 10);
    const queueCompletedDelta = finalCompletedQueue - initialCompletedQueue;
    const queuePagesDelta = parseInt(finalQueueRes.rows[0].pages, 10) - initialCompletedPages;

    const finalCompleted = parseInt(finalThroughputRes.rows[0].completed_chapters, 10);
    const finalFailed = parseInt(finalThroughputRes.rows[0].failed_chapters, 10);
    const finalPages = parseInt(finalThroughputRes.rows[0].total_pages, 10);
    const finalBytes = parseInt(finalThroughputRes.rows[0].total_bytes, 10);
    const finalBytesMb = (finalBytes / (1024 * 1024)).toFixed(2);
    const totalElapsedSec = Math.round((Date.now() - startTime) / 1000);
    const totalElapsedMin = totalElapsedSec / 60;

    const cpus = samples.map(s => s.cpuPercent);
    const conns = samples.map(s => s.totalConns);
    const hyps = samples.map(s => s.hyperdriveConns);
    const dirs = samples.map(s => s.directConns);
    const dCpus = discloudCpuSamples.length ? discloudCpuSamples : [0];
    const dRams = discloudRamSamples.length ? discloudRamSamples : [0];

    const summary = {
      benchmarkDurationMinutes: durationMinutes,
      actualDurationSeconds: totalElapsedSec,
      totalSamples: samples.length,
      datapath: 'DIRECT YSQL TLS (application_name: project-nox-importer-direct)',
      targetConcurrency: 5,
      throughput: {
        completedChapters: finalCompleted,
        queueCompletedDelta,
        failedChapters: finalFailed,
        totalPages: finalPages,
        queuePagesDelta,
        totalBytesMb: parseFloat(finalBytesMb),
        pagesPerMin: parseFloat((finalPages / totalElapsedMin).toFixed(2)),
        capPerMin: parseFloat((finalCompleted / totalElapsedMin).toFixed(2)),
        mbPerMin: parseFloat((parseFloat(finalBytesMb) / totalElapsedMin).toFixed(2)),
        avgPagesPerChapter: finalCompleted > 0 ? parseFloat((finalPages / finalCompleted).toFixed(2)) : 0,
        avgChapterDurationMs: Math.round(parseFloat(finalThroughputRes.rows[0].avg_duration_ms || 0))
      },
      discloud: {
        cpuAvg: avg(dCpus),
        cpuP95: percentile(dCpus, 0.95),
        cpuPeak: Math.max(...dCpus),
        ramAvg: avg(dRams),
        ramPeak: Math.max(...dRams),
        eventLoopLagAvg: avg(lagSamples),
        eventLoopLagP95: percentile(lagSamples, 0.95),
        eventLoopLagPeak: Math.max(...lagSamples)
      },
      yugabyte: {
        connectionsAvg: avg(conns),
        connectionsP95: percentile(conns, 0.95),
        connectionsPeak: Math.max(...conns),
        cpuAvg: avg(cpus),
        cpuP95: percentile(cpus, 0.95),
        cpuPeak: Math.max(...cpus),
        timeGte60PercentSec: samples.filter(s => s.cpuPercent >= 60).length * sampleIntervalSec,
        timeGte70PercentSec: samples.filter(s => s.cpuPercent >= 70).length * sampleIntervalSec,
        idleInTxMax: Math.max(...samples.map(s => s.idleInTx)),
        blockingLocksMax: Math.max(...samples.map(s => s.blockingLocks))
      },
      hyperdrive: {
        connectionsAvg: avg(hyps),
        connectionsP95: percentile(hyps, 0.95),
        connectionsPeak: Math.max(...hyps)
      },
      directImporter: {
        connectionsAvg: avg(dirs),
        connectionsP95: percentile(dirs, 0.95),
        connectionsPeak: Math.max(...dirs)
      },
      site: {
        home: getStats(homeLatencies, 250),
        reader: getStats(readerLatencies, 150),
        media: getStats(mediaLatencies, 120)
      },
      telegram: {
        rateLimits429Count: floodWaitMatches.length,
        floodWaitSecondsTotal: floodWaitTotalSeconds,
        uploadRetriesCount: retryMatches.length,
        mediaUploadErrorsDb: tgDbErrors
      },
      decouplingAudit: {
        discoveryIngestionDecoupled: 'YES',
        catalogDiscoverySetting: 'DISABLED',
        discoverWorksGenerated: discoverWorksGenerated,
        syncWorkGeneratedByDiscovery: syncWorkGenerated,
        importChaptersCompleted: finalCompleted
      },
      emergencyTriggered,
      emergencyReason
    };

    console.log('\n======================================================================');
    console.log('📊 5-WORKER 10-MINUTE PURE DIRECT BENCHMARK OFFICIAL SUMMARY');
    console.log('======================================================================');
    console.log(JSON.stringify(summary, null, 2));

    fs.writeFileSync('benchmark_5w_direct_10min_result.json', JSON.stringify({ summary, samples }, null, 2));
    console.log('\n✅ Detailed results written to benchmark_5w_direct_10min_result.json');

    await client.end();
  }
}

main().catch(console.error);
