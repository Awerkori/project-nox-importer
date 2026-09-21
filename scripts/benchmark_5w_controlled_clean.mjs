import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
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
  console.log('======================================================================');
  console.log('🚀 TESTE CONTROLADO DE 5 WORKERS — INGESTÃO EXCLUSIVA DE CAPÍTULOS');
  console.log('   Duração: 10 Minutos | Discovery Lane Isolada/Pausada');
  console.log('======================================================================');

  await client.connect();

  // 1. Check max_connections
  const maxConnRes = await client.query('SHOW max_connections');
  const maxConnections = parseInt(maxConnRes.rows[0].max_connections, 10);

  // 2. Open Publication Safety Barrier
  console.log('\n[Fase 1] Abrindo Publication Safety Barrier...');
  await client.query("UPDATE settings SET value = 'OPEN' WHERE key = 'publication_safety_barrier'");
  console.log('✅ Barreira de segurança ABERTA.');

  // 3. Release initial batch of chapter jobs if needed
  // Check how many chapters are already QUEUED
  const queuedChapsRes = await client.query(`
    SELECT count(*) as count
    FROM importer_queue
    WHERE status = 'QUEUED' AND task_type = 'IMPORT_CHAPTER'
  `);
  const queuedChaps = parseInt(queuedChapsRes.rows[0].count, 10);
  console.log(`[Fila] Capítulos já em QUEUED: ${queuedChaps}`);

  if (queuedChaps < 15) {
    const released = await client.query(`
      UPDATE importer_queue
      SET status = 'QUEUED', locked_by = NULL, locked_at = NULL, lease_expires_at = NULL, updated_at = NOW()
      WHERE id IN (
        SELECT id FROM importer_queue
        WHERE status = 'PAUSED_BY_STAFF'
          AND task_type = 'IMPORT_CHAPTER'
          AND source IN ('hanamiheaven', 'mangalivreto', 'fleurblanche', 'taimumangas')
        LIMIT 30
      )
      RETURNING id
    `);
    console.log(`[Fila] +${released.rowCount} capítulos liberados de PAUSED_BY_STAFF para QUEUED.`);
  }

  const testStartTime = Date.now();
  const testDurationMs = 10 * 60 * 1000;
  const testEndTime = testStartTime + testDurationMs;

  const ysqlConnections = [];
  const hyperdriveConns = [];
  const ybCpuSamples = [];
  const idleInTxSamples = [];
  const homeLatencies = [];
  const readerLatencies = [];
  const mediaLatencies = [];
  const discloudRssSamples = [];

  let highCpuDurationSec = 0;
  let emergencyTriggered = false;
  let emergencyReason = '';

  const initialCompletedRes = await client.query(`
    SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
    FROM importer_queue
    WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER'
  `);
  const initialCompletedCount = parseInt(initialCompletedRes.rows[0].count, 10);
  const initialPagesCount = parseInt(initialCompletedRes.rows[0].pages, 10);

  let sampleCount = 0;
  console.log('\n[Fase 2] Executando monitoramento contínuo de 10 minutos...');

  while (Date.now() < testEndTime) {
    sampleCount++;
    const now = Date.now();
    const elapsedSec = Math.round((now - testStartTime) / 1000);

    try {
      // Sample DB connections
      const connRes = await client.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
               count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
        FROM pg_stat_activity
      `);
      const total = parseInt(connRes.rows[0].total, 10);
      const hyperdrive = parseInt(connRes.rows[0].hyperdrive, 10);
      const idleInTx = parseInt(connRes.rows[0].idle_in_tx, 10);

      ysqlConnections.push(total);
      hyperdriveConns.push(hyperdrive);
      idleInTxSamples.push(idleInTx);

      // YB CPU
      let cpu = 0;
      try {
        const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
        const metrics = ybRes.rows[0]?.metrics || {};
        cpu = (parseFloat(metrics.cpu_usage_user || 0) + parseFloat(metrics.cpu_usage_system || 0)) * 100;
      } catch {}
      ybCpuSamples.push(cpu);
      if (cpu > 70) highCpuDurationSec += 6;

      // Probe Latencies every 30s
      if (sampleCount % 5 === 0) {
        homeLatencies.push(await probeLatency(HOME_URL, 3000));
        readerLatencies.push(await probeLatency(READER_URL, 3000));
        mediaLatencies.push(await probeLatency(MEDIA_URL, 3000));
      }

      // Telemetry Discloud
      const telemRes = await client.query(`
        SELECT rss_mb FROM importer_telemetry
        WHERE worker_id = 'discloud-importer-1'
        ORDER BY created_at DESC LIMIT 1
      `);
      if (telemRes.rows[0]) discloudRssSamples.push(parseFloat(telemRes.rows[0].rss_mb || 0));

      // Progress
      const curCompRes = await client.query(`
        SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
        FROM importer_queue
        WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER'
      `);
      const curCompleted = parseInt(curCompRes.rows[0].count, 10) - initialCompletedCount;
      const curPages = parseInt(curCompRes.rows[0].pages, 10) - initialPagesCount;

      const activeRes = await client.query(`
        SELECT count(*) FILTER (WHERE status = 'IMPORTING') as importing,
               count(*) FILTER (WHERE status = 'QUEUED') as queued
        FROM importer_queue
        WHERE task_type = 'IMPORT_CHAPTER'
      `);
      const importingCount = parseInt(activeRes.rows[0].importing, 10);
      const queuedCount = parseInt(activeRes.rows[0].queued, 10);

      // Progress log every 30s
      if (sampleCount % 5 === 0) {
        const elapsedMin = (elapsedSec / 60).toFixed(1);
        const pagesPerMin = elapsedSec > 0 ? ((curPages / elapsedSec) * 60).toFixed(1) : '0';
        const capPerMin = elapsedSec > 0 ? ((curCompleted / elapsedSec) * 60).toFixed(1) : '0';
        console.log(`[${elapsedMin}m / 10m] Conns: ${total}/${maxConnections} (Hyp: ${hyperdrive}) | YB CPU: ${cpu.toFixed(1)}% | Done: ${curCompleted} cap (${curPages} pág, ${pagesPerMin} p/m) | Active: ${importingCount}`);
      }

      // Safety checks
      if (total >= 13) {
        emergencyTriggered = true;
        emergencyReason = `CRITICAL: YSQL touched ${total}/${maxConnections}`;
        console.error(`🚨 ${emergencyReason}`);
        break;
      }
      if (total >= 12) {
        console.warn(`⚠️ [SAFETY WARNING] Conexões em ${total}/${maxConnections}. Fechando barreira temporariamente.`);
        await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");
      }
      if (cpu > 70 && highCpuDurationSec > 30) {
        console.warn(`⚠️ [SAFETY WARNING] CPU Yugabyte > 70% sustentada. Fechando barreira temporariamente.`);
        await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");
      }

    } catch (e) {
      console.error('Loop error:', e.message);
    }

    await new Promise(r => setTimeout(r, 6000));
  }

  const testActualDurationSec = Math.round((Date.now() - testStartTime) / 1000);
  console.log(`\n[Fase 3] Finalizando teste de 10 minutos (Duração real: ${testActualDurationSec}s)...`);

  // TEARDOWN:
  // 1. Close barrier
  await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");
  console.log('🔒 Barreira de segurança FECHADA.');

  // 2. Pause remaining queued chapters
  await client.query(`
    UPDATE importer_queue
    SET status = 'PAUSED_BY_STAFF',
        locked_by = NULL,
        locked_at = NULL,
        lease_expires_at = NULL,
        paused_by = '732fbe87-5040-41fb-9983-0aedb2af44c8',
        paused_at = NOW(),
        pause_reason = 'END_OF_10MIN_5W_TEST'
    WHERE status IN ('QUEUED', 'IMPORTING') AND task_type = 'IMPORT_CHAPTER'
  `);
  console.log('🛑 Capítulos remanescentes pausados.');

  // Final Counts
  const finalCompletedRes = await client.query(`
    SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
    FROM importer_queue
    WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER'
  `);
  const totalCompleted = parseInt(finalCompletedRes.rows[0].count, 10) - initialCompletedCount;
  const totalPages = parseInt(finalCompletedRes.rows[0].pages, 10) - initialPagesCount;

  const mediaBytesRes = await client.query(`
    SELECT COALESCE(sum(bytes), 0) as bytes
    FROM media
    WHERE created_at >= NOW() - INTERVAL '${Math.ceil(testActualDurationSec + 30)} seconds'
  `);
  const totalMb = (parseInt(mediaBytesRes.rows[0].bytes, 10) / (1024 * 1024)).toFixed(1);

  const durationMin = testActualDurationSec / 60;
  const finalPagesPerMin = (totalPages / durationMin).toFixed(1);
  const finalCapPerMin = (totalCompleted / durationMin).toFixed(2);
  const finalMbPerMin = (parseFloat(totalMb) / durationMin).toFixed(2);

  const summary = {
    testStatus: emergencyTriggered ? 'FAIL_EMERGENCY' : 'PASS',
    durationSec: testActualDurationSec,
    pagesPerMin: finalPagesPerMin,
    capPerMin: finalCapPerMin,
    mbPerMin: finalMbPerMin,
    totalChapters: totalCompleted,
    totalPages,
    totalMb,
    ysqlBaseline: `${avg(ysqlConnections)}/13`,
    ysqlPeak: `${Math.max(...ysqlConnections)}/13`,
    ysqlP95: `${percentile(ysqlConnections, 0.95)}/13`,
    hyperdrivePeak: Math.max(...hyperdriveConns),
    ybCpuAvg: `${avg(ybCpuSamples)}%`,
    ybCpuP95: `${percentile(ybCpuSamples, 0.95).toFixed(1)}%`,
    ybCpuPeak: `${Math.max(...ybCpuSamples).toFixed(1)}%`,
    timeCpuAbove70: `${highCpuDurationSec}s`,
    discloudRssAvg: `${avg(discloudRssSamples)} MB`,
    homeP95: `${percentile(homeLatencies.filter(x => x < 9000), 0.95)}ms`,
    readerP95: `${percentile(readerLatencies.filter(x => x < 9000), 0.95)}ms`,
    mediaP95: `${percentile(mediaLatencies.filter(x => x < 9000), 0.95)}ms`
  };

  fs.writeFileSync('benchmark_5w_controlled_clean_result.json', JSON.stringify(summary, null, 2));
  console.log('\n=== RESULTADO FINAL DO TESTE CONTROLADO ===');
  console.log(JSON.stringify(summary, null, 2));

  await client.end();
}

main().catch(err => {
  console.error('Fatal benchmark error:', err);
  process.exit(1);
});
