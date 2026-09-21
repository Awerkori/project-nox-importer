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

const HOME_URL = 'https://manga.project-nox-awerkori.workers.dev/';
const READER_URL = 'https://manga.project-nox-awerkori.workers.dev/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c';
const MEDIA_URL = 'https://manga.project-nox-awerkori.workers.dev/media/000003ed-c2db-4794-bcfa-c5e8b21ce080';

const agent = new https.Agent({ keepAlive: true, maxSockets: 10 });

function measureTTFB(url, timeoutMs = 4000) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = https.get(url, { agent }, (res) => {
      let resolved = false;
      res.once('data', () => {
        if (!resolved) {
          resolved = true;
          const ttfb = Math.round(performance.now() - t0);
          req.destroy();
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
    req.setTimeout(timeoutMs, () => { req.destroy(); resolve({ status: 408, ttfb: 9999 }); });
  });
}

const percentile = (arr, p) => {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
};

const avg = (arr) => arr.length ? (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1) : '0';

async function querySingleSnapshot() {
  const c = new Client(DB_CONFIG);
  await c.connect();
  try {
    const actRes = await c.query(`
      SELECT count(*) as total,
             count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
             count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
      FROM pg_stat_activity
    `);
    const ybRes = await c.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
    const m = ybRes.rows[0]?.metrics || {};
    const cpu = (parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100;
    return {
      total: parseInt(actRes.rows[0].total, 10),
      hyperdrive: parseInt(actRes.rows[0].hyperdrive, 10),
      idleInTx: parseInt(actRes.rows[0].idle_in_tx, 10),
      cpu
    };
  } finally {
    await c.end();
  }
}

async function main() {
  console.log('======================================================================');
  console.log('🔬 INVESTIGAÇÃO DE BASELINE: PASSIVO (5 MIN) vs INSTRUMENTADO (5 MIN)');
  console.log('======================================================================');

  // Warmup keep-alive agent once
  await measureTTFB(HOME_URL);
  await measureTTFB(READER_URL);
  await measureTTFB(MEDIA_URL);

  // ====================================================================
  // JANELA A: 5 MINUTOS PASSIVO
  // Amostragem espaçada (a cada 30s), sem probes agressivos, sem conexão persistente
  // ====================================================================
  console.log('\n>>> [FASE 1] INICIANDO JANELA A: 5 MINUTOS PASSIVO...');
  console.log('Critério: Sem auditoria de 5s, sem loops pesados, 1 probe a cada 60s com TTFB/keep-alive.\n');

  const passConns = [];
  const passHyp = [];
  const passCpu = [];
  const passIdleInTx = [];
  const passHome = [];
  const passReader = [];
  const passMedia = [];

  const passStartTime = Date.now();
  const passDuration = 5 * 60 * 1000;
  let passSampleCount = 0;

  while (Date.now() - passStartTime < passDuration) {
    passSampleCount++;
    const snap = await querySingleSnapshot();
    passConns.push(snap.total);
    passHyp.push(snap.hyperdrive);
    passCpu.push(snap.cpu);
    passIdleInTx.push(snap.idleInTx);

    // 1 probe a cada 60s (a cada 2 amostras de 30s)
    if (passSampleCount % 2 === 0) {
      const h = await measureTTFB(HOME_URL);
      const r = await measureTTFB(READER_URL);
      const m = await measureTTFB(MEDIA_URL);
      passHome.push(h.ttfb);
      passReader.push(r.ttfb);
      passMedia.push(m.ttfb);
      console.log(`[Passivo ${(passSampleCount * 30 / 60).toFixed(1)}m] YSQL: ${snap.total}/13 (Hyp: ${snap.hyperdrive}) | CPU: ${snap.cpu.toFixed(1)}% | TTFB -> H: ${h.ttfb}ms, R: ${r.ttfb}ms, M: ${m.ttfb}ms`);
    } else {
      console.log(`[Passivo ${(passSampleCount * 30 / 60).toFixed(1)}m] YSQL: ${snap.total}/13 (Hyp: ${snap.hyperdrive}) | CPU: ${snap.cpu.toFixed(1)}%`);
    }

    await new Promise(r => setTimeout(r, 30000));
  }

  const passiveResult = {
    ysqlAvg: avg(passConns),
    ysqlP95: percentile(passConns, 0.95),
    ysqlPeak: Math.max(...passConns),
    hypAvg: avg(passHyp),
    hypPeak: Math.max(...passHyp),
    cpuAvg: avg(passCpu),
    cpuP95: percentile(passCpu, 0.95).toFixed(1),
    cpuPeak: Math.max(...passCpu).toFixed(1),
    maxIdleInTx: Math.max(...passIdleInTx),
    homeP95: percentile(passHome, 0.95),
    readerP95: percentile(passReader, 0.95),
    mediaP95: percentile(passMedia, 0.95),
    samplesCount: passConns.length
  };
  console.log('\n--- RESULTADO JANELA A (PASSIVO) ---');
  console.log(JSON.stringify(passiveResult, null, 2));

  // ====================================================================
  // JANELA B: 5 MINUTOS INSTRUMENTADO
  // Telemetria do benchmark: conexão persistente, amostragem a cada 6s, probes a cada 30s
  // ====================================================================
  console.log('\n>>> [FASE 2] INICIANDO JANELA B: 5 MINUTOS INSTRUMENTADO...');
  console.log('Critério: Conexão persistente pg.Client, query pg_stat_activity a cada 6s, probes a cada 30s.\n');

  const instClient = new Client(DB_CONFIG);
  await instClient.connect();

  const instConns = [];
  const instHyp = [];
  const instCpu = [];
  const instIdleInTx = [];
  const instHome = [];
  const instReader = [];
  const instMedia = [];
  const cpuSpikes = [];

  const instStartTime = Date.now();
  const instDuration = 5 * 60 * 1000;
  let instSampleCount = 0;

  try {
    while (Date.now() - instStartTime < instDuration) {
      instSampleCount++;
      const actRes = await instClient.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
               count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
        FROM pg_stat_activity
      `);
      const total = parseInt(actRes.rows[0].total, 10);
      const hyp = parseInt(actRes.rows[0].hyperdrive, 10);
      const idleInTx = parseInt(actRes.rows[0].idle_in_tx, 10);

      const ybRes = await instClient.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
      const m = ybRes.rows[0]?.metrics || {};
      const cpu = (parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100;

      instConns.push(total);
      instHyp.push(hyp);
      instIdleInTx.push(idleInTx);
      instCpu.push(cpu);

      // If CPU spikes > 70%, immediately inspect pg_stat_activity to capture the culprit query
      if (cpu > 70) {
        const culpritRes = await instClient.query(`
          SELECT pid, application_name, state, query
          FROM pg_stat_activity
          WHERE state = 'active' OR application_name = 'Cloudflare Hyperdrive'
        `);
        cpuSpikes.push({
          timestamp: new Date().toISOString(),
          cpu: cpu.toFixed(1) + '%',
          totalConns: total,
          culprits: culpritRes.rows.map(r => ({ app: r.application_name, state: r.state, query: (r.query || '').slice(0, 100) }))
        });
      }

      // Probes every 30s (every 5 samples of 6s)
      if (instSampleCount % 5 === 0) {
        const h = await measureTTFB(HOME_URL);
        const r = await measureTTFB(READER_URL);
        const m = await measureTTFB(MEDIA_URL);
        instHome.push(h.ttfb);
        instReader.push(r.ttfb);
        instMedia.push(m.ttfb);
        console.log(`[Instrumentado ${(instSampleCount * 6 / 60).toFixed(1)}m] YSQL: ${total}/13 (Hyp: ${hyp}) | CPU: ${cpu.toFixed(1)}% | TTFB -> H: ${h.ttfb}ms, R: ${r.ttfb}ms, M: ${m.ttfb}ms`);
      } else if (instSampleCount % 5 === 1) {
        console.log(`[Instrumentado ${(instSampleCount * 6 / 60).toFixed(1)}m] YSQL: ${total}/13 (Hyp: ${hyp}) | CPU: ${cpu.toFixed(1)}%`);
      }

      await new Promise(r => setTimeout(r, 6000));
    }
  } finally {
    await instClient.end();
  }

  const instrumentedResult = {
    ysqlAvg: avg(instConns),
    ysqlP95: percentile(instConns, 0.95),
    ysqlPeak: Math.max(...instConns),
    hypAvg: avg(instHyp),
    hypPeak: Math.max(...instHyp),
    cpuAvg: avg(instCpu),
    cpuP95: percentile(instCpu, 0.95).toFixed(1),
    cpuPeak: Math.max(...instCpu).toFixed(1),
    maxIdleInTx: Math.max(...instIdleInTx),
    homeP95: percentile(instHome, 0.95),
    readerP95: percentile(instReader, 0.95),
    mediaP95: percentile(instMedia, 0.95),
    samplesCount: instConns.length,
    cpuSpikesCount: cpuSpikes.length,
    cpuSpikesSample: cpuSpikes.slice(0, 3)
  };

  console.log('\n--- RESULTADO JANELA B (INSTRUMENTADO) ---');
  console.log(JSON.stringify(instrumentedResult, null, 2));

  const comparison = {
    passive: passiveResult,
    instrumented: instrumentedResult
  };

  fs.writeFileSync('baseline_comparison_result.json', JSON.stringify(comparison, null, 2));
  console.log('\n✅ Comparação completa salva em baseline_comparison_result.json.');
}

main().catch(err => {
  console.error('Fatal comparison error:', err);
  process.exit(1);
});
