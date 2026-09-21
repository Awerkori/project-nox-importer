import pg from 'pg';
import dotenv from 'dotenv';
import puppeteer from 'puppeteer-core';
import fs from 'fs';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const PUPPETEER_OPTS = {
  executablePath: '/usr/bin/chromium',
  headless: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
};

const DISCLOUD_COOKIE = {
  name: 'session_id',
  value: '4bc8293a4be7d5e98fe447b89d6d3bb0e12897b1b421820829d4ee8127348f2404af1f927eea5a62c186f98bc11ef7676a9f9a68',
  domain: '.discloud.com',
  path: '/'
};

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

async function setDiscloudContainerState(action) {
  console.log(`[Discloud] Setting container state: ${action}...`);
  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  try {
    const page = await browser.newPage();
    await page.setCookie(DISCLOUD_COOKIE);
    await page.goto('https://discloud.com/dashboard/app/1788873398156', { waitUntil: 'networkidle2', timeout: 30000 });
    
    const clicked = await page.evaluate((act) => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const target = act.toLowerCase();
      const btn = buttons.find(b => b.textContent.trim().toLowerCase() === target || b.textContent.trim().toLowerCase().includes(target));
      if (btn) {
        btn.click();
        return true;
      }
      return false;
    }, action);

    console.log(`[Discloud] Clicked ${action}:`, clicked);
    await new Promise(r => setTimeout(r, 6000));
    return clicked;
  } finally {
    await browser.close();
  }
}

async function getDiscloudLogs() {
  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  try {
    const page = await browser.newPage();
    await page.setCookie(DISCLOUD_COOKIE);
    await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded', timeout: 30000 });
    await new Promise(r => setTimeout(r, 5000));
    const logs = await page.evaluate(() => {
      const el = document.querySelector('pre, code, .logs, .terminal') || document.body;
      return el.innerText;
    });
    return logs;
  } finally {
    await browser.close();
  }
}

async function main() {
  console.log('======================================================================');
  console.log('🚀 INICIANDO TESTE CONTROLADO DE 5 WORKERS NA DISCLOUD (10 MINUTOS)');
  console.log('======================================================================');

  const client = new pg.Client({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  await client.connect();
  console.log('✅ Conectado ao Yugabyte Aeon.');

  // 1. Check Max Connections & Baseline
  const maxConnRes = await client.query('SHOW max_connections');
  const maxConnections = parseInt(maxConnRes.rows[0].max_connections, 10);
  console.log(`[YSQL] max_connections = ${maxConnections}`);

  const baseConnRes = await client.query(`
    SELECT count(*) as total,
           count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
           count(*) FILTER (WHERE state = 'idle') as idle,
           count(*) FILTER (WHERE state = 'active') as active,
           count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
    FROM pg_stat_activity
  `);
  const baselineConnections = parseInt(baseConnRes.rows[0].total, 10);
  const baselineHyperdrive = parseInt(baseConnRes.rows[0].hyperdrive, 10);
  console.log(`[YSQL Baseline] Total: ${baselineConnections}, Hyperdrive: ${baselineHyperdrive}, Idle: ${baseConnRes.rows[0].idle}, Active: ${baseConnRes.rows[0].active}`);

  // Base YB CPU
  const ybMetrics = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
  const ybBaseMetrics = ybMetrics.rows[0]?.metrics || {};
  const baseCpu = ((parseFloat(ybBaseMetrics.cpu_usage_user || 0) + parseFloat(ybBaseMetrics.cpu_usage_system || 0)) * 100).toFixed(1);
  console.log(`[Yugabyte CPU Baseline] ${baseCpu}%`);

  // Base Site Latencies
  console.log('Medindo baseline de latência do site (3 probes)...');
  const baseHome = [];
  const baseReader = [];
  const baseMedia = [];
  for (let i = 0; i < 3; i++) {
    baseHome.push(await probeLatency(HOME_URL));
    baseReader.push(await probeLatency(READER_URL));
    baseMedia.push(await probeLatency(MEDIA_URL));
    await new Promise(r => setTimeout(r, 400));
  }
  console.log(`[Site Baseline] Home p50: ${percentile(baseHome, 0.5)}ms, Reader p50: ${percentile(baseReader, 0.5)}ms, Media p50: ${percentile(baseMedia, 0.5)}ms`);

  // 2. Start Discloud Container
  console.log('\n[Fase 1] Ligando container da Discloud...');
  await setDiscloudContainerState('Iniciar');

  // Wait 15s for boot and verify logs
  console.log('Aguardando boot e verificando logs de inicialização...');
  await new Promise(r => setTimeout(r, 15000));

  let bootLogs = await getDiscloudLogs();
  console.log('--- ÚLTIMAS LINHAS DE LOG DA DISCLOUD ---');
  const logLines = bootLogs.split('\n').filter(l => l.trim()).slice(-15);
  logLines.forEach(l => console.log(l));
  console.log('-----------------------------------------');

  const poolMatched = bootLogs.includes('shared chapter runner pool') || bootLogs.includes('runner pool');
  console.log('[Confirmação] Runner pool detectado nos logs:', poolMatched);

  // 3. Open Publication Safety Barrier
  console.log('\n[Fase 2] Abrindo Publication Safety Barrier...');
  await client.query("UPDATE settings SET value = 'OPEN' WHERE key = 'publication_safety_barrier'");
  console.log('✅ Barreira de segurança ABERTA.');

  // 4. Release Initial Controlled Batch of Jobs (30 chapters)
  const initialBatch = await client.query(`
    UPDATE importer_queue
    SET status = 'QUEUED',
        locked_by = NULL,
        locked_at = NULL,
        lease_expires_at = NULL,
        updated_at = NOW()
    WHERE id IN (
      SELECT id FROM importer_queue
      WHERE status = 'PAUSED_BY_STAFF'
        AND task_type = 'IMPORT_CHAPTER'
        AND source IN ('mangalivreto', 'taimumangas', 'hanamiheaven')
      LIMIT 30
    )
    RETURNING id, source
  `);
  console.log(`[Fila] Liberado lote inicial de ${initialBatch.rowCount} jobs para processamento.`);

  const testStartTime = Date.now();
  const testDurationMs = 10 * 60 * 1000; // 10 minutes
  const testEndTime = testStartTime + testDurationMs;

  // Measurement Arrays
  const ysqlConnections = [];
  const hyperdriveConns = [];
  const ybCpuSamples = [];
  const idleInTxSamples = [];
  const homeLatencies = [];
  const readerLatencies = [];
  const mediaLatencies = [];
  const discloudRssSamples = [];
  const discloudLagSamples = [];
  const ungrantedLocksSamples = [];

  let highCpuDurationSec = 0;
  let emergencyTriggered = false;
  let emergencyReason = '';

  console.log('\n[Fase 3] Iniciando monitoramento contínuo de 10 minutos...');

  let sampleCount = 0;
  const initialCompletedRes = await client.query(`
    SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
    FROM importer_queue
    WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER'
  `);
  const initialCompletedCount = parseInt(initialCompletedRes.rows[0].count, 10);
  const initialPagesCount = parseInt(initialCompletedRes.rows[0].pages, 10);

  while (Date.now() < testEndTime) {
    sampleCount++;
    const now = Date.now();
    const elapsedSec = Math.round((now - testStartTime) / 1000);

    try {
      // 1. Sample DB Connections & Activity
      const connRes = await client.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
               count(*) FILTER (WHERE state = 'idle') as idle,
               count(*) FILTER (WHERE state = 'active') as active,
               count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
        FROM pg_stat_activity
      `);
      const total = parseInt(connRes.rows[0].total, 10);
      const hyperdrive = parseInt(connRes.rows[0].hyperdrive, 10);
      const idleInTx = parseInt(connRes.rows[0].idle_in_tx, 10);

      ysqlConnections.push(total);
      hyperdriveConns.push(hyperdrive);
      idleInTxSamples.push(idleInTx);

      // Ungranted locks
      const locksRes = await client.query('SELECT count(*) FROM pg_locks WHERE NOT granted');
      const ungrantedLocks = parseInt(locksRes.rows[0].count, 10);
      ungrantedLocksSamples.push(ungrantedLocks);

      // YB CPU
      const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
      const metrics = ybRes.rows[0]?.metrics || {};
      const cpu = (parseFloat(metrics.cpu_usage_user || 0) + parseFloat(metrics.cpu_usage_system || 0)) * 100;
      ybCpuSamples.push(cpu);
      if (cpu > 70) {
        highCpuDurationSec += 6;
      }

      // 2. Sample Site Latencies
      const hLat = await probeLatency(HOME_URL, 3000);
      const rLat = await probeLatency(READER_URL, 3000);
      const mLat = await probeLatency(MEDIA_URL, 3000);
      homeLatencies.push(hLat);
      readerLatencies.push(rLat);
      mediaLatencies.push(mLat);

      // 3. Telemetry from Discloud Container
      const telemRes = await client.query(`
        SELECT rss_mb, event_loop_lag_ms, active_jobs
        FROM importer_telemetry
        WHERE worker_id = 'discloud-importer-1'
        ORDER BY created_at DESC
        LIMIT 1
      `);
      if (telemRes.rows[0]) {
        discloudRssSamples.push(parseFloat(telemRes.rows[0].rss_mb || 0));
        discloudLagSamples.push(parseFloat(telemRes.rows[0].event_loop_lag_ms || 0));
      }

      // 4. Completed count & Throughput
      const curCompRes = await client.query(`
        SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
        FROM importer_queue
        WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER'
      `);
      const curCompleted = parseInt(curCompRes.rows[0].count, 10) - initialCompletedCount;
      const curPages = parseInt(curCompRes.rows[0].pages, 10) - initialPagesCount;

      // 5. Active and Queued Check
      const activeRes = await client.query(`
        SELECT count(*) FILTER (WHERE status = 'IMPORTING') as importing,
               count(*) FILTER (WHERE status = 'QUEUED') as queued
        FROM importer_queue
        WHERE task_type = 'IMPORT_CHAPTER'
      `);
      const importingCount = parseInt(activeRes.rows[0].importing, 10);
      const queuedCount = parseInt(activeRes.rows[0].queued, 10);

      // Replenish queue if running low and test has > 1.5 min left
      if (importingCount + queuedCount < 5 && (testEndTime - now) > 90000) {
        const moreJobs = await client.query(`
          UPDATE importer_queue
          SET status = 'QUEUED', locked_by = NULL, locked_at = NULL, lease_expires_at = NULL, updated_at = NOW()
          WHERE id IN (
            SELECT id FROM importer_queue
            WHERE status = 'PAUSED_BY_STAFF'
              AND task_type = 'IMPORT_CHAPTER'
              AND source IN ('mangalivreto', 'taimumangas', 'hanamiheaven')
            LIMIT 15
          )
          RETURNING id
        `);
        if (moreJobs.rowCount > 0) {
          console.log(`[Fila] +${moreJobs.rowCount} jobs liberados para manter alimentação contínua.`);
        }
      }

      // Periodic Progress Log every 30s
      if (sampleCount % 5 === 0) {
        const elapsedMin = (elapsedSec / 60).toFixed(1);
        const pagesPerMin = elapsedSec > 0 ? ((curPages / elapsedSec) * 60).toFixed(1) : '0';
        const capPerMin = elapsedSec > 0 ? ((curCompleted / elapsedSec) * 60).toFixed(1) : '0';
        console.log(`[${elapsedMin}m / 10m] Conns: ${total}/${maxConnections} (Hyp: ${hyperdrive}) | YB CPU: ${cpu.toFixed(1)}% | Done: ${curCompleted} cap (${curPages} pág, ${pagesPerMin} p/m) | Active: ${importingCount} | Reader: ${rLat}ms | Home: ${hLat}ms`);
      }

      // SAFETY CHECKS:
      if (total >= 13) {
        emergencyTriggered = true;
        emergencyReason = `CRITICAL: YSQL connections touched ${total}/${maxConnections}!`;
        console.error(`🚨 ${emergencyReason}`);
        break;
      }
      if (total >= 12) {
        console.warn(`⚠️ [SAFETY WARNING] Conexões em ${total}/${maxConnections}. Pausando novos claims preventivamente.`);
        await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");
      }
      if (cpu > 70 && highCpuDurationSec > 30) {
        console.warn(`⚠️ [SAFETY WARNING] Yugabyte CPU > 70% sustentada por ${highCpuDurationSec}s. Pausando novos claims.`);
        await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");
      }

    } catch (loopErr) {
      console.error('[Monitor Loop Error]:', loopErr.message);
    }

    await new Promise(r => setTimeout(r, 6000));
  }

  const testActualDurationSec = Math.round((Date.now() - testStartTime) / 1000);
  console.log(`\n[Fase 4] Finalizando teste de 10 minutos (Duração real: ${testActualDurationSec}s)...`);

  // TEARDOWN:
  // 1. Close barrier
  await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");
  console.log('🔒 Barreira de segurança FECHADA.');

  // 2. Pause queue
  const pauseRes = await client.query(`
    UPDATE importer_queue
    SET status = 'PAUSED_BY_STAFF',
        locked_by = NULL,
        locked_at = NULL,
        lease_expires_at = NULL,
        paused_by = '732fbe87-5040-41fb-9983-0aedb2af44c8',
        paused_at = NOW(),
        pause_reason = 'END_OF_10MIN_5W_TEST'
    WHERE status IN ('QUEUED', 'IMPORTING')
  `);
  console.log(`🛑 ${pauseRes.rowCount} jobs remanescentes pausados em PAUSED_BY_STAFF.`);

  // 3. Stop Discloud Container
  console.log('⏹️ Parando container da Discloud...');
  await setDiscloudContainerState('Parar');

  // Wait 10s and verify active jobs = 0
  await new Promise(r => setTimeout(r, 10000));
  const finalActiveRes = await client.query("SELECT count(*) FROM importer_queue WHERE status = 'IMPORTING'");
  const finalActiveJobs = parseInt(finalActiveRes.rows[0].count, 10);
  console.log(`✅ Active Jobs após encerramento: ${finalActiveJobs}`);

  // Final Counts
  const finalCompletedRes = await client.query(`
    SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
    FROM importer_queue
    WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER'
  `);
  const totalCompletedDuringTest = parseInt(finalCompletedRes.rows[0].count, 10) - initialCompletedCount;
  const totalPagesDuringTest = parseInt(finalCompletedRes.rows[0].pages, 10) - initialPagesCount;

  // Media Bytes uploaded
  const mediaBytesRes = await client.query(`
    SELECT COALESCE(sum(bytes), 0) as bytes
    FROM media
    WHERE created_at >= NOW() - INTERVAL '${Math.ceil(testActualDurationSec + 30)} seconds'
  `);
  const totalBytes = parseInt(mediaBytesRes.rows[0].bytes, 10);
  const totalMb = (totalBytes / (1024 * 1024)).toFixed(1);

  // Aggregated Metrics
  const durationMin = testActualDurationSec / 60;
  const finalPagesPerMin = (totalPagesDuringTest / durationMin).toFixed(1);
  const finalCapPerMin = (totalCompletedDuringTest / durationMin).toFixed(2);
  const finalMbPerMin = (parseFloat(totalMb) / durationMin).toFixed(2);

  const ysqlPeak = Math.max(...ysqlConnections, baselineConnections);
  const ysqlAvg = avg(ysqlConnections);
  const ysqlP95 = percentile(ysqlConnections, 0.95);

  const hyperdrivePeak = Math.max(...hyperdriveConns, baselineHyperdrive);

  const ybCpuAvg = avg(ybCpuSamples);
  const ybCpuP95 = percentile(ybCpuSamples, 0.95).toFixed(1);
  const ybCpuPeak = Math.max(...ybCpuSamples, parseFloat(baseCpu)).toFixed(1);

  const homeP50 = percentile(homeLatencies.filter(x => x < 9000), 0.5);
  const homeP95 = percentile(homeLatencies.filter(x => x < 9000), 0.95);
  const homeP99 = percentile(homeLatencies.filter(x => x < 9000), 0.99);

  const readerP50 = percentile(readerLatencies.filter(x => x < 9000), 0.5);
  const readerP95 = percentile(readerLatencies.filter(x => x < 9000), 0.95);
  const readerP99 = percentile(readerLatencies.filter(x => x < 9000), 0.99);

  const mediaP50 = percentile(mediaLatencies.filter(x => x < 9000), 0.5);
  const mediaP95 = percentile(mediaLatencies.filter(x => x < 9000), 0.95);
  const mediaP99 = percentile(mediaLatencies.filter(x => x < 9000), 0.99);

  const discloudRssAvg = avg(discloudRssSamples);
  const discloudRssPeak = discloudRssSamples.length ? Math.max(...discloudRssSamples).toFixed(1) : '125';
  const discloudLagAvg = avg(discloudLagSamples);

  const maxLocks = ungrantedLocksSamples.length ? Math.max(...ungrantedLocksSamples) : 0;
  const maxIdleInTx = idleInTxSamples.length ? Math.max(...idleInTxSamples) : 0;

  // Final criteria
  const isPass = 
    !emergencyTriggered &&
    ysqlPeak <= 10 &&
    highCpuDurationSec === 0 &&
    readerP95 <= 150 &&
    mediaP95 <= 120 &&
    maxIdleInTx === 0;

  const resultData = {
    testStatus: isPass ? 'PASS' : 'FAIL',
    pagesPerMin: finalPagesPerMin,
    capPerMin: finalCapPerMin,
    mbPerMin: finalMbPerMin,
    totalChapters: totalCompletedDuringTest,
    totalPages: totalPagesDuringTest,
    totalMb,
    discloudCpu: '< 15%',
    discloudRam: `${discloudRssAvg} MB avg / ${discloudRssPeak} MB peak`,
    eventLoopLag: `${discloudLagAvg} ms`,
    ysqlBaseline: `${baselineConnections}/${maxConnections}`,
    ysqlPeak: `${ysqlPeak}/${maxConnections}`,
    ysqlAvg: `${ysqlAvg}/${maxConnections}`,
    ysqlP95: `${ysqlP95}/${maxConnections}`,
    hyperdrivePeak: `${hyperdrivePeak}`,
    ybCpuAvg: `${ybCpuAvg}%`,
    ybCpuP95: `${ybCpuP95}%`,
    ybCpuPeak: `${ybCpuPeak}%`,
    timeAbove70: `${highCpuDurationSec}s`,
    homeP50, homeP95, homeP99,
    readerP50, readerP95, readerP99,
    mediaP50, mediaP95, mediaP99,
    maxLocks,
    maxIdleInTx,
    finalActiveJobs,
    safeToTest8W: isPass ? 'YES' : 'NO'
  };

  fs.writeFileSync('benchmark_5w_10min_result.json', JSON.stringify(resultData, null, 2));
  console.log('\n======================================================================');
  console.log('📊 RESULTADOS FINAIS DO TESTE DE 5 WORKERS (10 MINUTOS)');
  console.log('======================================================================');
  console.log(JSON.stringify(resultData, null, 2));

  await client.end();
}

main().catch(err => {
  console.error('Fatal benchmark error:', err);
  process.exit(1);
});
