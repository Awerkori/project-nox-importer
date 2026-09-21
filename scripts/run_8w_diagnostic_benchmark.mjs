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
  if (!arr || !arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
};

const avg = (arr) => arr && arr.length ? (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1) : '0';

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
  const durationMinutes = parseFloat(process.argv[2] || '3.5');
  const sampleIntervalSec = parseInt(process.argv[3] || '10', 10);
  const durationMs = durationMinutes * 60 * 1000;

  const sessionId = `diag-8w-${Date.now()}`;

  console.log('======================================================================');
  console.log(`🔬 PROJECT NOX — 8-WORKER DIRECT PIPELINE DIAGNOSTIC BENCHMARK (${durationMinutes} MINUTES)`);
  console.log(`   Session ID: ${sessionId}`);
  console.log(`   Datapath: DIRECT YSQL TLS -> YugabyteDB (Zero Gateway / Hyperdrive)`);
  console.log(`   Scope: IMPORT_CHAPTER only | Discovery: OFF | Reconciler: ACTIVE`);
  console.log(`   Goal: Pure Telemetry & Bottleneck Location (No limits changed)`);
  console.log('======================================================================\n');

  const client = new Client(DB_CONFIG);
  await client.connect();

  const maxConnRes = await client.query('SHOW max_connections');
  const maxConnections = parseInt(maxConnRes.rows[0].max_connections, 10);
  console.log(`[Config] YSQL max_connections = ${maxConnections}`);

  console.log('Warming up persistent HTTP connections...');
  const wHome = await measureTTFB(HOME_URL, homeAgent);
  const wReader = await measureTTFB(READER_URL, readerAgent);
  const wMedia = await measureTTFB(MEDIA_URL, mediaAgent);
  console.log(`Warmup completed: Home=${wHome.ttfb}ms, Reader=${wReader.ttfb}ms, Media=${wMedia.ttfb}ms\n`);

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

  // Step 1: Ensure discovery is disabled globally and on all sources
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

  // Step 3: Register active diagnostic session in settings
  console.log(`[Phase 1] Activating diagnostic session [${sessionId}] in importer...`);
  await client.query(`
    INSERT INTO settings (key, value)
    VALUES ('active_diagnostic_session', $1)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
  `, [sessionId]);

  // Step 4: Open Publication Safety Barrier
  console.log('[Phase 1] Opening Publication Safety Barrier...');
  await client.query("UPDATE settings SET value = 'OPEN' WHERE key = 'publication_safety_barrier'");
  console.log('✅ Publication Safety Barrier is OPEN. 8-Worker pool will start acquiring chapters.\n');

  const startTime = Date.now();
  const samples = [];
  const homeLatencies = [];
  const readerLatencies = [];
  const mediaLatencies = [];
  const discloudCpuSamples = [];
  const discloudRamSamples = [];

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

      const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
      const m = ybRes.rows[0]?.metrics || {};
      const cpu = (parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100;

      // 2. Queue state
      const queueRes = await client.query(`
        SELECT count(*) FILTER (WHERE status = 'IMPORTING') as importing_now,
               count(*) FILTER (WHERE status = 'QUEUED') as queued_now
        FROM importer_queue
        WHERE task_type = 'IMPORT_CHAPTER'
      `);
      const importingNow = parseInt(queueRes.rows[0].importing_now, 10);
      const queuedNow = parseInt(queueRes.rows[0].queued_now, 10);

      // 3. Ingestion progress from importer_job_metrics
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

      // 4. Site latency probes
      const homeRes = await measureTTFB(HOME_URL, homeAgent);
      const readerRes = await measureTTFB(READER_URL, readerAgent);
      const mediaRes = await measureTTFB(MEDIA_URL, mediaAgent);

      if (homeRes.ttfb < 9000) homeLatencies.push(homeRes.ttfb);
      if (readerRes.ttfb < 9000) readerLatencies.push(readerRes.ttfb);
      if (mediaRes.ttfb < 9000) mediaLatencies.push(mediaRes.ttfb);

      // 5. Discloud API live metrics
      const discloud = await getDiscloudStatus();
      if (discloud.cpu > 0) discloudCpuSamples.push(discloud.cpu);
      if (discloud.ram > 0) discloudRamSamples.push(discloud.ram);

      // 6. Live Diagnostic Telemetry from importer
      const diagRes = await client.query(`
        SELECT data FROM importer_diagnostic_telemetry
        WHERE session_id = $1
        ORDER BY created_at DESC LIMIT 1
      `, [sessionId]);
      const diagData = diagRes.rows[0]?.data || {};
      const activeWorkersAvg = diagData.activeWorkers?.avg || 0;
      const activeWorkersPeak = diagData.activeWorkers?.peak || 0;
      const dbPoolWaitAvg = diagData.yugabyteDbPool?.waitAvgMs || 0;
      const dbPoolWaitP95 = diagData.yugabyteDbPool?.waitP95Ms || 0;

      const sample = {
        idx: sampleIdx,
        timestamp: new Date().toISOString(),
        elapsedSec,
        totalConns: parseInt(actRes.rows[0].total, 10),
        hyperdriveConns: parseInt(actRes.rows[0].hyperdrive, 10),
        directConns: parseInt(actRes.rows[0].direct_importer, 10),
        activeConns: parseInt(actRes.rows[0].active, 10),
        idleInTx: parseInt(actRes.rows[0].idle_in_tx, 10),
        cpuPercent: parseFloat(cpu.toFixed(2)),
        discloudCpu: discloud.cpu,
        discloudRam: discloud.ram,
        importingNow,
        queuedNow,
        completedChapters,
        totalPages,
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
        mediaTtfb: mediaRes.ttfb
      };
      samples.push(sample);

      console.log(
        `[#${sampleIdx} ${elapsedSec}s/${Math.round(durationMs/1000)}s] ` +
        `Workers: ${importingNow} in queue | Active Diag: avg ${activeWorkersAvg} (peak ${activeWorkersPeak}) | ` +
        `Done: ${completedChapters} cap (${capPerMin} c/m, ${pagesPerMin} p/m, ${mbPerMin} MB/m) | ` +
        `DB Pool Wait: avg ${dbPoolWaitAvg}ms (p95: ${dbPoolWaitP95}ms) | ` +
        `YB CPU: ${sample.cpuPercent.toFixed(1)}% | YSQL: ${sample.totalConns}/${maxConnections} | ` +
        `Discloud: ${sample.discloudCpu}% CPU, ${sample.discloudRam}MB | Site TTFB: H:${sample.homeTtfb}ms R:${sample.readerTtfb}ms`
      );

      // Tripwire check
      if (sample.totalConns >= 13) {
        emergencyTriggered = true;
        emergencyReason = `CRITICAL: YSQL touched max capacity ${sample.totalConns}/${maxConnections}`;
        console.error(`🚨 ${emergencyReason}`);
        break;
      }

      const elapsed = Date.now() - t0;
      const waitTime = Math.max(100, (sampleIntervalSec * 1000) - elapsed);
      await new Promise(r => setTimeout(r, waitTime));
    }
  } finally {
    console.log('\n======================================================================');
    console.log('🛑 DIAGNOSTIC BENCHMARK COMPLETED — INITIATING RAMP DOWN');
    console.log('======================================================================');

    // 1. Close Publication Safety Barrier
    console.log('[Rampdown 1] Closing Publication Safety Barrier...');
    await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");
    console.log('✅ Publication Safety Barrier CLOSED.');

    // 2. Pause chapter sources
    console.log('[Rampdown 2] Pausing chapter sources...');
    await client.query(`
      UPDATE importer_sources 
      SET status = 'PAUSED', updated_at = NOW() 
      WHERE id IN ('hanamiheaven', 'fleurblanche', 'mangalivreto')
    `);
    console.log('✅ Chapter sources PAUSED.');

    // 3. Await active chapter jobs to drain (max 60s)
    console.log('[Rampdown 3] Waiting for in-flight chapter jobs to finish (max 60s)...');
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
        console.log(`[Rampdown] In-flight jobs remaining: ${activeCh}... waiting 2s`);
        await new Promise(r => setTimeout(r, 2000));
      }
    }
    console.log(`✅ In-flight chapter jobs reached: ${activeCh}`);

    // 4. Mark diagnostic session complete in settings
    await client.query(`
      UPDATE settings SET value = 'IDLE' WHERE key = 'active_diagnostic_session'
    `);

    // 5. Fetch final diagnostic report from importer_diagnostic_telemetry
    console.log('[Telemetry] Pulling full diagnostic telemetry dataset from Yugabyte...');
    const finalDiagRes = await client.query(`
      SELECT data FROM importer_diagnostic_telemetry
      WHERE session_id = $1
      ORDER BY created_at DESC LIMIT 1
    `, [sessionId]);

    const diag = finalDiagRes.rows[0]?.data || {};

    // 6. Fetch chapters from importer_job_metrics for this session window
    const chRes = await client.query(`
      SELECT source, chapter_number, page_count, total_bytes, duration_ms, download_ms, upload_ms, db_ms, status, created_at
      FROM importer_job_metrics
      WHERE created_at >= $1
      ORDER BY created_at ASC
    `, [benchmarkStartTimeIso]);

    // 7. Save raw diagnostic JSON
    const outputJson = {
      summary: {
        sessionId,
        durationMinutes,
        actualDurationSeconds: Math.round((Date.now() - startTime) / 1000),
        benchmarkStartTimeIso,
        benchmarkEndTimeIso: new Date().toISOString(),
        totalChaptersCompleted: chRes.rows.filter(r => r.status === 'COMPLETED').length,
        totalChaptersFailed: chRes.rows.filter(r => r.status === 'FAILED').length,
        totalPages: chRes.rows.reduce((a, b) => a + parseInt(b.page_count || 0, 10), 0),
        totalBytes: chRes.rows.reduce((a, b) => a + parseInt(b.total_bytes || 0, 10), 0),
      },
      telemetry: diag,
      jobMetrics: chRes.rows,
      samples,
    };

    fs.writeFileSync(
      '/home/awerkori/.Projects/project-nox-importer/benchmark_8w_diagnostic_result.json',
      JSON.stringify(outputJson, null, 2)
    );
    console.log('✅ Saved full telemetry to benchmark_8w_diagnostic_result.json\n');

    await client.end();
  }
}

main().catch(err => {
  console.error('Fatal error during diagnostic benchmark:', err);
  process.exit(1);
});
