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

const avg = (arr) => arr.length ? (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1) : '0';

async function main() {
  await client.connect();

  console.log('=== ETAPA 1: DETALHE DOS JOBS EM QUEUED ANTES DA PAUSA ===');
  const queuedDetail = await client.query(`
    SELECT task_type, source, count(*) as count
    FROM importer_queue
    WHERE status = 'QUEUED'
    GROUP BY task_type, source
    ORDER BY task_type, count DESC
  `);
  console.table(queuedDetail.rows);

  const queuedSummary = await client.query(`
    SELECT task_type, count(*) as count
    FROM importer_queue
    WHERE status = 'QUEUED'
    GROUP BY task_type
  `);
  console.table(queuedSummary.rows);

  console.log('\n=== ETAPA 2: PAUSANDO DISCOVERY LANE COM PRESERVAÇÃO INTEGRAL ===');
  // 1. Temporarily pause active sources in importer_sources
  const pauseSources = await client.query(`
    UPDATE importer_sources
    SET status = 'PAUSED', updated_at = NOW()
    WHERE status = 'ACTIVE'
    RETURNING id
  `);
  console.log(`Fontes pausadas temporariamente: ${pauseSources.rows.map(r => r.id).join(', ')}`);

  // 2. Temporarily pause queued DISCOVER_WORKS and SYNC_WORK jobs
  const pauseQueue = await client.query(`
    UPDATE importer_queue
    SET status = 'PAUSED_BY_STAFF',
        pause_reason = 'PAUSED_FOR_5W_BENCHMARK',
        updated_at = NOW()
    WHERE status IN ('QUEUED', 'RETRY')
      AND task_type IN ('DISCOVER_WORKS', 'SYNC_WORK')
    RETURNING id, task_type, source
  `);
  console.log(`Jobs de discovery pausados com segurança: ${pauseQueue.rowCount} (marcados com PAUSED_FOR_5W_BENCHMARK)`);

  // 3. Aguardar qualquer job in-flight de discovery finalizar
  console.log('Aguardando in-flight jobs finalizarem (até 15s)...');
  let activeJobsCount = 1;
  let attempts = 0;
  while (activeJobsCount > 0 && attempts < 15) {
    const actRes = await client.query("SELECT count(*) as count FROM importer_queue WHERE status = 'IMPORTING'");
    activeJobsCount = parseInt(actRes.rows[0].count, 10);
    if (activeJobsCount > 0) {
      console.log(`Ainda há ${activeJobsCount} job(s) em execução... aguardando 2s`);
      await new Promise(r => setTimeout(r, 2000));
      attempts++;
    }
  }

  // 4. Confirmar estado pré-teste
  const barrierRes = await client.query("SELECT value FROM settings WHERE key = 'publication_safety_barrier'");
  const barrier = barrierRes.rows[0]?.value || 'UNKNOWN';

  const finalActiveRes = await client.query(`
    SELECT count(*) as total,
           count(*) FILTER (WHERE task_type IN ('DISCOVER_WORKS', 'SYNC_WORK')) as active_discovery,
           count(*) FILTER (WHERE task_type = 'IMPORT_CHAPTER') as active_chapter
    FROM importer_queue
    WHERE status = 'IMPORTING'
  `);
  const activeStats = finalActiveRes.rows[0];

  console.log('\n=== ESTADO PRÉ-TESTE ===');
  console.log(`DISCOVERY ACTIVE: ${activeStats.active_discovery}`);
  console.log(`ACTIVE DISCOVER_WORKS: ${activeStats.active_discovery}`);
  console.log(`ACTIVE CHAPTER JOBS: ${activeStats.active_chapter}`);
  console.log(`PUBLICATION BARRIER: ${barrier}`);
  console.log(`RUNNER SLOTS: 5`);
  console.log(`GATEWAY MAX CONCURRENCY: 4`);

  console.log('\n=== ETAPA 3: MEDINDO BASELINE LIMPO POR 2 MINUTOS (24 AMOSTRAS A CADA 5s) ===');
  const ysqlConns = [];
  const hyperdriveConns = [];
  const cpuSamples = [];
  const idleInTxSamples = [];

  const homeProbes = [];
  const readerProbes = [];
  const mediaProbes = [];

  const startTime = Date.now();
  // 2 minutes = 120s -> 24 samples
  for (let i = 0; i < 24; i++) {
    const actRes = await client.query(`
      SELECT count(*) as total,
             count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
             count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
      FROM pg_stat_activity
    `);
    const total = parseInt(actRes.rows[0].total, 10);
    const hyp = parseInt(actRes.rows[0].hyperdrive, 10);
    const idleInTx = parseInt(actRes.rows[0].idle_in_tx, 10);

    let cpu = 0;
    try {
      const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
      const m = ybRes.rows[0]?.metrics || {};
      cpu = (parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100;
    } catch {}

    ysqlConns.push(total);
    hyperdriveConns.push(hyp);
    idleInTxSamples.push(idleInTx);
    cpuSamples.push(cpu);

    // Probe latencies every 15 seconds
    if (i % 3 === 0) {
      homeProbes.push(await probeLatency(HOME_URL));
      readerProbes.push(await probeLatency(READER_URL));
      mediaProbes.push(await probeLatency(MEDIA_URL));
    }

    process.stdout.write(`[Baseline ${i + 1}/24] Conns: ${total}/13 (Hyp: ${hyp}) | CPU: ${cpu.toFixed(1)}%\r`);
    await new Promise(r => setTimeout(r, 5000));
  }
  console.log('\nBaseline concluído!');

  // Extra latency probes to have at least 15 probes
  for (let j = 0; j < 7; j++) {
    homeProbes.push(await probeLatency(HOME_URL));
    readerProbes.push(await probeLatency(READER_URL));
    mediaProbes.push(await probeLatency(MEDIA_URL));
    await new Promise(r => setTimeout(r, 150));
  }

  // Baseline metrics
  const ysqlPeak = Math.max(...ysqlConns);
  const ysqlMin = Math.min(...ysqlConns);
  const ysqlAvgVal = avg(ysqlConns);

  const hypPeak = Math.max(...hyperdriveConns);
  const hypMin = Math.min(...hyperdriveConns);
  const hypAvgVal = avg(hyperdriveConns);

  const cpuAvgVal = avg(cpuSamples);
  const cpuP95Val = percentile(cpuSamples, 0.95).toFixed(1);
  const cpuPeakVal = Math.max(...cpuSamples).toFixed(1);

  const homeP95Val = percentile(homeProbes.filter(x => x < 9000), 0.95);
  const readerP95Val = percentile(readerProbes.filter(x => x < 9000), 0.95);
  const mediaP95Val = percentile(mediaProbes.filter(x => x < 9000), 0.95);

  const cleanBaselineSummary = {
    ysqlBaseline: `${ysqlAvgVal}/13 (min ${ysqlMin}, peak ${ysqlPeak})`,
    hyperdriveConns: `${hypAvgVal} avg (min ${hypMin}, peak ${hypPeak})`,
    maxIdleInTx: Math.max(...idleInTxSamples),
    cpuMetrics: `avg ${cpuAvgVal}% / p95 ${cpuP95Val}% / peak ${cpuPeakVal}%`,
    homeP95: `${homeP95Val}ms`,
    readerP95: `${readerP95Val}ms`,
    mediaP95: `${mediaP95Val}ms`
  };

  console.log('\n=== RESULTADO DO BASELINE LIMPO (2 MINUTOS) ===');
  console.log(JSON.stringify(cleanBaselineSummary, null, 2));

  await client.end();
}

main().catch(err => {
  console.error('Error during prepare_clean_5w:', err);
  process.exit(1);
});
