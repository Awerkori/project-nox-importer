import fs from 'fs';
import path from 'path';
import { getYugabytePool } from '../build/db/yugabyte-direct.js';

// Parse command line arguments
const args = process.argv.slice(2);
function getArg(key, defaultValue) {
  const idx = args.indexOf(key);
  if (idx !== -1 && idx + 1 < args.length) return args[idx + 1];
  return defaultValue;
}

const windowLabel = getArg('--window', '03h00-03h30');
const cpNum = parseInt(getArg('--cp', '24'), 10);
const targetUtcStr = getArg('--target-utc', '2026-11-02T06:30:00.000Z');
const targetUtc = new Date(targetUtcStr);
const startTimeBrt = getArg('--start-brt', '03h00 BRT');
const endTimeBrt = getArg('--end-brt', '03h30 BRT');
const responsibles = getArg('--responsibles', 'Antigravity + Codex');
const activities = getArg('--activities', 'SQL (Aeon <=9 admin), Site (sub-1s Playwright), P0 (43 fontes), P1 (600 cap/h meta)');

const SCRATCH_DIR = '/home/awerkori/.Projects/project-nox-importer/scratch';
if (!fs.existsSync(SCRATCH_DIR)) fs.mkdirSync(SCRATCH_DIR, { recursive: true });
const STATE_FILE = path.join(SCRATCH_DIR, 'active_tasks_state.json');

const pool = getYugabytePool();
import { telemetryCollector } from '../build/core/telemetry-collector.js';
telemetryCollector.stop();
const startTime = Date.now();

console.log('╔════════════════════════════════════════════════════════════════════════════╗');
console.log(`║ PROJECT NOX — Checkpoint ${windowLabel}                                    ║`);
console.log('╠════════════════════════════════════════════════════════════════════════════╣');
console.log(`║ Início:                ${startTimeBrt.padEnd(52)}║`);
console.log(`║ Encerramento previsto: ${endTimeBrt.padEnd(52)}║`);
console.log(`║ Estado:                EXECUTANDO / OBSERVANDO                             ║`);
console.log(`║ Responsáveis:          ${responsibles.padEnd(52)}║`);
console.log(`║ Atividades:            ${activities.padEnd(52)}║`);
console.log('╚════════════════════════════════════════════════════════════════════════════╝');

let iteration = 0;

async function sampleAndLog() {
  iteration++;
  const now = new Date();
  const elapsedSec = Math.floor((Date.now() - startTime) / 1000);
  const remainingSec = Math.max(0, Math.floor((targetUtc.getTime() - Date.now()) / 1000));
  const remainingMin = (remainingSec / 60).toFixed(1);

  let sqlInfo = { total: 0, active: 0, idle: 0, admin: 0 };
  let p1Info = { caps15m: 0, caps30m: 0, caps60m: 0, ratePerHour: 0 };
  let p0Info = { cycleId: 'none', sourcesInspected: 0, totalSources: 43, canonicalWorks: 0, eligibleWorks: 0 };
  let runningTasks = 0;

  let client;
  try {
    client = await pool.connect();

    // 1. Conexões SQL
    const connRes = await client.query(`
      SELECT usename, state, count(*) as count
      FROM pg_stat_activity
      WHERE datname = current_database()
      GROUP BY usename, state;
    `);
    for (const r of connRes.rows) {
      const c = parseInt(r.count, 10);
      sqlInfo.total += c;
      if (r.state === 'active') sqlInfo.active += c;
      if (r.state === 'idle') sqlInfo.idle += c;
      if (r.usename === 'admin') sqlInfo.admin += c;
    }

    // 2. Tarefas e Throughput
    const taskRes = await client.query(`
      SELECT count(*) as running FROM importer_lite_tasks WHERE state = 'RUNNING';
    `);
    runningTasks = parseInt(taskRes.rows[0]?.running || '0', 10);

    const pubRes = await client.query(`
      SELECT 
        count(*) FILTER (WHERE bucket_minute >= date_trunc('minute', now() - interval '15 minutes')) as c15m,
        count(*) FILTER (WHERE bucket_minute >= date_trunc('minute', now() - interval '30 minutes')) as c30m,
        count(*) FILTER (WHERE bucket_minute >= date_trunc('minute', now() - interval '60 minutes')) as c60m
      FROM importer_publication_events
      WHERE bucket_minute >= date_trunc('minute', now() - interval '60 minutes');
    `);
    if (pubRes.rows.length > 0) {
      p1Info.caps15m = parseInt(pubRes.rows[0].c15m || '0', 10);
      p1Info.caps30m = parseInt(pubRes.rows[0].c30m || '0', 10);
      p1Info.caps60m = parseInt(pubRes.rows[0].c60m || '0', 10);
      p1Info.ratePerHour = p1Info.caps15m * 4;
    }

    // 3. P0 Cycle
    const p0Res = await client.query(`
      SELECT cycle_id, status, sources_inspected, total_eligible_works, 
             COALESCE((metadata->'metrics'->>'canonicalWorks')::int, total_eligible_works) as canonical_works
      FROM importer_p0_cycles
      ORDER BY started_at DESC
      LIMIT 1;
    `);
    if (p0Res.rows.length > 0) {
      const row = p0Res.rows[0];
      p0Info.cycleId = (row.cycle_id || '').slice(0, 8);
      p0Info.eligibleWorks = row.total_eligible_works || 0;
      p0Info.sourcesInspected = row.sources_inspected || 0;
      p0Info.canonicalWorks = row.canonical_works || row.total_eligible_works || 0;
    }
  } catch (err) {
    console.error(`[Monitor Error] ${err.message}`);
  } finally {
    if (client) client.release();
  }

  // Update shared active task state
  const taskState = {
    taskName: `PROJECT NOX — Checkpoint ${windowLabel}`,
    cpNum,
    window: windowLabel,
    startTime: startTimeBrt,
    endTime: endTimeBrt,
    targetUtc: targetUtc.toISOString(),
    state: remainingSec > 0 ? 'EXECUTANDO / OBSERVANDO' : 'CONCLUIDO',
    responsibles,
    activities,
    elapsedSec,
    remainingMin: `${remainingMin}m`,
    updatedAt: now.toISOString(),
    metrics: {
      sql: { total: sqlInfo.total, active: sqlInfo.active, idle: sqlInfo.idle, admin: sqlInfo.admin, limitAdmin: 9, limitTotal: 13 },
      p0: { cycleId: p0Info.cycleId, sourcesInspected: p0Info.sourcesInspected, totalSources: 43, canonicalWorks: p0Info.canonicalWorks, eligibleWorks: p0Info.eligibleWorks },
      p1: { caps15m: p1Info.caps15m, caps30m: p1Info.caps30m, caps60m: p1Info.caps60m, ratePerHour: p1Info.ratePerHour, targetPerHour: 600 },
      importerQueue: { runningTasks }
    }
  };
  fs.writeFileSync(STATE_FILE, JSON.stringify(taskState, null, 2), 'utf8');

  // Print single line progress to stdout
  const brtTime = new Date(Date.now() - 3 * 3600 * 1000).toISOString().substring(11, 19);
  console.log(`[${brtTime} BRT | T+${elapsedSec}s | Restam ${remainingMin}m] SQL: ${sqlInfo.total}/13 (admin: ${sqlInfo.admin}/9, act: ${sqlInfo.active}) | P0: ${p0Info.sourcesInspected}/43 srcs | P1: ${p1Info.ratePerHour} cap/h (60m: ${p1Info.caps60m}) | Tasks: ${runningTasks} | Status: OBSERVANDO`);

  if (Date.now() >= targetUtc.getTime()) {
    console.log(`\n================================================================`);
    console.log(`[PROJECT NOX] Janela ${windowLabel} atingiu o horário previsto (${endTimeBrt}).`);
    console.log(`Total 60m publicado: ${p1Info.caps60m} caps. Conexões admin finais: ${sqlInfo.admin}/9.`);
    console.log(`Pronto para disparo e registro do Checkpoint ${cpNum}.`);
    console.log(`================================================================\n`);
    await pool.end();
    process.exit(0);
  }
}

// Initial sample
sampleAndLog();

// Interval every 10 seconds
const timer = setInterval(() => {
  sampleAndLog().catch((e) => console.error(e));
}, 10000);

process.on('SIGINT', async () => {
  clearInterval(timer);
  await pool.end();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  clearInterval(timer);
  await pool.end();
  process.exit(0);
});
