import path from 'node:path';
import process from 'node:process';
import { atomicWriteJson, fetchWithTimeout, monotonicMs, nowIso } from './monitor-utils.mjs';
import { probeYugabyte } from './postgres-probe.mjs';

const heartbeatFile = process.env.MONITOR_HEARTBEAT_FILE ?? path.resolve(process.cwd(), '.monitor/heartbeat.json');
const resultFile = process.env.MONITOR_RESULT_FILE ?? path.resolve(process.cwd(), '.monitor/result.json');
const taskId = process.env.MONITOR_TASK_ID ?? `monitor-cycle-${process.pid}`;
const startedAt = nowIso();
const startedMono = monotonicMs();
const maxCycleMs = Number(process.env.MONITOR_WORKER_DEADLINE_MS ?? 60000);
const deadline = new Date(Date.now() + maxCycleMs).toISOString();
let heartbeat = {
  task_id: taskId,
  pid: process.pid,
  started_at: startedAt,
  last_progress_at: startedAt,
  MONITOR_LAST_PROGRESS_AT: startedAt,
  deadline,
  status: 'running',
  stage: 'start',
};

async function progress(stage, extra = {}) {
  const progressAt = nowIso();
  heartbeat = { ...heartbeat, ...extra, task_id: taskId, pid: process.pid, last_progress_at: progressAt, MONITOR_LAST_PROGRESS_AT: progressAt, stage };
  await atomicWriteJson(heartbeatFile, heartbeat);
}

function routeSummary(sample) {
  return {
    status: sample.status === 200 ? 'PASS' : sample.status === null ? 'UNKNOWN' : 'FAIL',
    statusCode: sample.status,
    durationMs: sample.durationMs,
    finalUrl: sample.finalUrl,
    error: sample.error,
  };
}

async function probeSite() {
  const base = process.env.NOX_MANGA_URL ?? 'https://manga.project-nox-awerkori.workers.dev';
  const readerPath = process.env.MONITOR_READER_PATH ?? '/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c';
  const samples = {};
  for (const [name, url] of [['home', `${base}/`], ['reader', `${base}${readerPath}`]]) {
    const sample = await fetchWithTimeout(url, { timeoutMs: 5000, headers: { 'User-Agent': 'ProjectNox-Vigilancia/1.0' } });
    samples[name] = routeSummary(sample);
  }
  return {
    status: Object.values(samples).some((sample) => sample.status === 'FAIL') ? 'FAIL' : Object.values(samples).some((sample) => sample.status === 'UNKNOWN') ? 'UNKNOWN' : 'PASS',
    samples,
    http5xx: Object.values(samples).filter((sample) => Number(sample.statusCode) >= 500).length,
  };
}

async function probeTelegram() {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) return { status: 'UNKNOWN', reason: 'TELEGRAM_BOT_TOKEN unavailable' };
  const sample = await fetchWithTimeout(`https://api.telegram.org/bot${token}/getMe`, { timeoutMs: 5000 });
  return {
    status: sample.status === 200 ? 'PASS' : sample.status === null ? 'UNKNOWN' : 'FAIL',
    statusCode: sample.status,
    durationMs: sample.durationMs,
    error: sample.error,
    rateLimited: sample.status === 429,
    serverError: Number(sample.status) >= 500,
    timedOut: sample.error === 'TIMEOUT',
  };
}

function summarizeAdaptive(yugabyte) {
  const adaptive = yugabyte.adaptive ?? { status: 'UNKNOWN', events: [] };
  const events = Array.isArray(adaptive.events) ? adaptive.events : [];
  const classify = (event) => String(event?.type ?? event?.action ?? event?.decision ?? '').toLowerCase();
  return {
    status: adaptive.status ?? 'UNKNOWN',
    state: adaptive.state ?? null,
    scaleUp: events.filter((event) => /scale.?up|increase|ramp/.test(classify(event))).length,
    scaleDown: events.filter((event) => /scale.?down|decrease|cooldown|survival/.test(classify(event))).length,
    recovery: events.filter((event) => /recover|healthy|resume/.test(classify(event))).length,
    events,
  };
}

async function main() {
  await progress('observe');
  const y = await probeYugabyte();
  await progress('site', { metrics: { yugabyte: y } });
  const site = await probeSite();
  await progress('telegram', { metrics: { yugabyte: y, site } });
  const telegram = await probeTelegram();
  const result = {
    taskId,
    pid: process.pid,
    startedAt,
    finishedAt: nowIso(),
    durationMs: Math.round(monotonicMs() - startedMono),
    importer: y.importer ?? { status: 'UNKNOWN' },
    site,
    yugabyte: y,
    telegram,
    adaptive: summarizeAdaptive(y),
  };
  await atomicWriteJson(resultFile, result);
  await progress('complete', { status: 'complete', result: { status: 'complete', finished_at: result.finishedAt } });
}

main().catch(async (error) => {
  const result = { taskId, pid: process.pid, startedAt, finishedAt: nowIso(), status: 'FAIL', error: String(error?.message ?? error) };
  try { await atomicWriteJson(resultFile, result); } catch {}
  try { await progress('error', { status: 'error', error: result.error }); } catch {}
  process.exitCode = 1;
});
