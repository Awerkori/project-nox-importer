import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { atomicWriteJson, monotonicMs, nowIso, readJson, sleep } from './monitor-utils.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(here, '../..');
const workerScript = path.join(here, 'monitor-worker.mjs');
const fixtureScript = path.join(here, 'watchdog-fixture.mjs');

function numberArg(args, name, fallback) {
  const index = args.indexOf(name);
  if (index < 0) return fallback;
  const value = Number(args[index + 1]);
  return Number.isFinite(value) ? value : fallback;
}

function stringArg(args, name, fallback) {
  const index = args.indexOf(name);
  return index >= 0 && args[index + 1] ? args[index + 1] : fallback;
}

function parseOptions(args = process.argv.slice(2)) {
  const selfTest = args.includes('--self-test');
  const stateDir = stringArg(args, '--state-dir', process.env.MONITOR_STATE_DIR ?? '/tmp/project-nox-vigilancia');
  return {
    selfTest,
    stateDir,
    stateFile: stringArg(args, '--state-file', path.join(stateDir, 'vigilancia-state.json')),
    heartbeatFile: stringArg(args, '--heartbeat-file', path.join(stateDir, 'heartbeat.json')),
    resultFile: stringArg(args, '--result-file', path.join(stateDir, 'result.json')),
    targetEffectiveSeconds: numberArg(args, '--target-effective-seconds', 9 * 60 * 60),
    warningMs: numberArg(args, '--warning-ms', 5 * 60 * 1000),
    killMs: numberArg(args, '--kill-ms', 10 * 60 * 1000),
    workerDeadlineMs: numberArg(args, '--worker-deadline-ms', 60 * 1000),
    cycleDelayMs: numberArg(args, '--cycle-delay-ms', 30 * 1000),
    checkpointMs: numberArg(args, '--checkpoint-ms', 15 * 60 * 1000),
    staleCheckpointMs: numberArg(args, '--stale-checkpoint-ms', 20 * 60 * 1000),
    tickMs: numberArg(args, '--tick-ms', 5000),
    once: args.includes('--once'),
  };
}

function emptyState(options, existing = null) {
  const now = nowIso();
  const priorInvalidSeconds = Number(process.env.MONITOR_PRIOR_INVALID_SECONDS ?? 0);
  const priorEffectiveSeconds = Number(process.env.MONITOR_PRIOR_EFFECTIVE_SECONDS ?? 0);
  return existing ?? {
    schemaVersion: 1,
    watchStartedAt: now,
    targetEffectiveSeconds: options.targetEffectiveSeconds,
    originalWatchElapsedSeconds: priorInvalidSeconds + priorEffectiveSeconds,
    effectiveSeconds: priorEffectiveSeconds,
    invalidSeconds: priorInvalidSeconds,
    lastCheckpointAt: null,
    lastProgressAt: now,
    lastSupervisorHeartbeatAt: now,
    supervisorPid: process.pid,
    currentStatus: 'starting',
    adaptiveEvents: [],
    incidents: priorInvalidSeconds > 0 ? [{ type: 'preexisting_invalid_interval', seconds: priorInvalidSeconds, recordedAt: now, reason: 'previously reported hang; treated as invalid until proven otherwise' }] : [],
    checkpoints: [],
    snapshots: [],
  };
}

async function processIsAlive(child) {
  if (!child || child.exitCode !== null || child.signalCode) return false;
  try { process.kill(child.pid, 0); return true; } catch { return false; }
}

function readHeartbeatAgeMs(heartbeat, fallbackStartedAt = null) {
  const timestamp = Date.parse(heartbeat?.last_progress_at ?? heartbeat?.MONITOR_LAST_PROGRESS_AT ?? '');
  if (Number.isFinite(timestamp)) return Math.max(0, Date.now() - timestamp);
  if (fallbackStartedAt) return Math.max(0, Date.now() - fallbackStartedAt);
  return Number.POSITIVE_INFINITY;
}

async function terminateChild(child, graceMs) {
  if (!child || !(await processIsAlive(child))) return { terminated: false, killed: false };
  child.kill('SIGTERM');
  const started = monotonicMs();
  while (await processIsAlive(child) && monotonicMs() - started < graceMs) await sleep(25);
  if (await processIsAlive(child)) {
    child.kill('SIGKILL');
    return { terminated: true, killed: true };
  }
  return { terminated: true, killed: false };
}

export async function runSupervisor(rawOptions = {}) {
  const options = { ...parseOptions([]), ...rawOptions };
  await fs.mkdir(options.stateDir, { recursive: true });
  let state = emptyState(options, await readJson(options.stateFile));
  state.targetEffectiveSeconds = options.targetEffectiveSeconds;
  const priorSupervisorHeartbeat = Date.parse(state.lastSupervisorHeartbeatAt ?? '');
  const priorGapMs = Number.isFinite(priorSupervisorHeartbeat) ? Math.max(0, Date.now() - priorSupervisorHeartbeat) : 0;
  let child = null;
  let childStartedAt = null;
  let childExit = null;
  let nextSpawnAt = 0;
  let lastTickMono = monotonicMs();
  let lastCheckpointWall = state.lastCheckpointAt ? Date.parse(state.lastCheckpointAt) : 0;
  let selfTestKilled = false;
  let stopping = false;

  const persist = async () => {
    state.supervisorPid = process.pid;
    state.remainingEffectiveSeconds = Math.max(0, state.targetEffectiveSeconds - state.effectiveSeconds);
    state.lastSupervisorHeartbeatAt = nowIso();
    await atomicWriteJson(options.stateFile, state);
  };

  const recordIncident = (incident) => {
    state.incidents.push({ ...incident, recordedAt: nowIso() });
    if (state.incidents.length > 500) state.incidents.splice(0, state.incidents.length - 500);
  };

  if (priorGapMs > Math.max(options.tickMs * 2, 10_000)) {
    state.invalidSeconds += priorGapMs / 1000;
    recordIncident({ type: 'unobserved_supervisor_gap', durationMs: priorGapMs, reason: 'previous supervisor heartbeat was stale at restart' });
  }

  const attachChild = (nextChild, taskId) => {
    child = nextChild;
    childStartedAt = Date.now();
    childExit = null;
    state.currentStatus = 'task_running';
    state.task = { taskId, pid: child.pid, startedAt: new Date(childStartedAt).toISOString(), deadline: new Date(childStartedAt + options.workerDeadlineMs).toISOString(), status: 'running' };
    child.stdout?.on('data', (data) => process.stdout.write(`[worker ${taskId}] ${String(data)}`));
    child.stderr?.on('data', (data) => process.stderr.write(`[worker ${taskId} stderr] ${String(data)}`));
    child.once('exit', (code, signal) => {
      childExit = { code, signal, at: nowIso() };
      state.lastProgressAt = childExit.at;
      state.task = { ...(state.task ?? {}), status: code === 0 ? 'complete' : 'failed', exitCode: code, signal, finishedAt: childExit.at };
      child = null;
      childStartedAt = null;
      nextSpawnAt = Date.now() + options.cycleDelayMs;
    });
  };

  const spawnWorker = () => {
    const taskId = `${options.selfTest ? 'self-test' : 'cycle'}-${Date.now()}`;
    const env = {
      ...process.env,
      MONITOR_TASK_ID: taskId,
      MONITOR_HEARTBEAT_FILE: options.heartbeatFile,
      MONITOR_RESULT_FILE: options.resultFile,
      MONITOR_WORKER_DEADLINE_MS: String(options.workerDeadlineMs),
    };
    const command = options.selfTest
      ? [fixtureScript, 'hang', String(Math.max(options.killMs * 4, 1000))]
      : [workerScript];
    const nextChild = spawn(process.execPath, command, { cwd: projectRoot, env, stdio: ['ignore', 'pipe', 'pipe'] });
    attachChild(nextChild, taskId);
  };

  const saveCheckpoint = async (reason = 'interval') => {
    const result = await readJson(options.resultFile, null);
    const checkpoint = {
      timestamp: nowIso(),
      reason,
      effectiveSeconds: Math.floor(state.effectiveSeconds),
      invalidSeconds: Math.floor(state.invalidSeconds),
      importer: result?.importer ?? { status: 'UNKNOWN', reason: 'no completed worker snapshot' },
      site: result?.site ?? { status: 'UNKNOWN', reason: 'no completed worker snapshot' },
      yugabyte: result?.yugabyte ?? { status: 'UNKNOWN', reason: 'no completed worker snapshot' },
      telegram: result?.telegram ?? { status: 'UNKNOWN', reason: 'no completed worker snapshot' },
      adaptive: result?.adaptive ?? { status: 'UNKNOWN', events: [] },
      incidentsSincePrevious: state.incidents.slice(-10),
    };
    state.checkpoints.push(checkpoint);
    state.snapshots.push(result);
    if (state.checkpoints.length > 1000) state.checkpoints.splice(0, state.checkpoints.length - 1000);
    if (state.snapshots.length > 1000) state.snapshots.splice(0, state.snapshots.length - 1000);
    state.lastCheckpointAt = checkpoint.timestamp;
    lastCheckpointWall = Date.now();
    const adaptiveEvents = Array.isArray(checkpoint.adaptive?.events) ? checkpoint.adaptive.events : [];
    state.adaptiveEvents.push(...adaptiveEvents);
    if (state.adaptiveEvents.length > 500) state.adaptiveEvents.splice(0, state.adaptiveEvents.length - 500);
    await persist();
    return checkpoint;
  };

  const onSignal = () => { stopping = true; };
  process.once('SIGTERM', onSignal);
  process.once('SIGINT', onSignal);

  try {
    await persist();
    if (!child) spawnWorker();
    while (!stopping && state.effectiveSeconds < options.targetEffectiveSeconds) {
      const nowMono = monotonicMs();
      const deltaSeconds = Math.max(0, Math.min(30, (nowMono - lastTickMono) / 1000));
      lastTickMono = nowMono;
      const heartbeat = await readJson(options.heartbeatFile, null);
      const taskHeartbeat = heartbeat?.task_id === state.task?.taskId ? heartbeat : null;
      const heartbeatAge = readHeartbeatAgeMs(taskHeartbeat, childStartedAt);
      const taskAlive = await processIsAlive(child);
      const taskDeadline = Date.parse(state.task?.deadline ?? '');
      const deadlineExceeded = Boolean(child && Number.isFinite(taskDeadline) && Date.now() > taskDeadline);
      const taskStale = Boolean(child && (!taskAlive || heartbeatAge > options.warningMs || deadlineExceeded));

      if (taskStale) {
        state.invalidSeconds += deltaSeconds;
        state.currentStatus = heartbeatAge > options.killMs ? 'task_timeout' : 'task_warning';
        if (heartbeatAge > options.warningMs && !state.incidents.some((incident) => incident.taskId === state.task?.taskId && incident.type === 'no_progress_warning')) {
          recordIncident({ type: 'no_progress_warning', taskId: state.task?.taskId, ageMs: Number.isFinite(heartbeatAge) ? heartbeatAge : null, progressMissing: !Number.isFinite(heartbeatAge), thresholdMs: options.warningMs });
        }
        if ((heartbeatAge > options.killMs || deadlineExceeded) && child) {
          recordIncident({ type: deadlineExceeded ? 'task_deadline_killed' : 'task_killed', taskId: state.task?.taskId, ageMs: Number.isFinite(heartbeatAge) ? heartbeatAge : null, progressMissing: !Number.isFinite(heartbeatAge), thresholdMs: deadlineExceeded ? options.workerDeadlineMs : options.killMs, recoveryAction: 'fresh_bounded_worker_cycle' });
          const termination = await terminateChild(child, 1000);
          selfTestKilled ||= termination.killed || termination.terminated;
          nextSpawnAt = Date.now() + options.cycleDelayMs;
        }
      } else {
        state.effectiveSeconds += deltaSeconds;
        state.currentStatus = child ? 'task_running' : 'waiting_next_cycle';
        if (taskHeartbeat?.last_progress_at ?? taskHeartbeat?.MONITOR_LAST_PROGRESS_AT) {
          state.lastProgressAt = taskHeartbeat.last_progress_at ?? taskHeartbeat.MONITOR_LAST_PROGRESS_AT;
        }
      }

      if (!child && Date.now() >= nextSpawnAt && !options.once) spawnWorker();
      if (!child && options.once && !selfTestKilled) spawnWorker();

      if (lastCheckpointWall && Date.now() - lastCheckpointWall >= options.staleCheckpointMs) {
        recordIncident({ type: 'checkpoint_stale', ageMs: Date.now() - lastCheckpointWall, thresholdMs: options.staleCheckpointMs });
        await saveCheckpoint('stale-checkpoint-recovery');
      } else if (!lastCheckpointWall || Date.now() - lastCheckpointWall >= options.checkpointMs) {
        await saveCheckpoint('interval');
      }

      await persist();
      if (options.selfTest && selfTestKilled && !child) break;
      await sleep(options.tickMs);
    }

    if (options.selfTest) {
      const pass = selfTestKilled && state.incidents.some((incident) => incident.type === 'task_killed');
      state.selfTest = { passed: pass, finishedAt: nowIso() };
      await saveCheckpoint('self-test');
      await persist();
      return { ...state, selfTest: state.selfTest };
    }

    if (state.effectiveSeconds >= options.targetEffectiveSeconds) state.currentStatus = 'completed';
    await saveCheckpoint('final');
    await persist();
    return state;
  } finally {
    process.removeListener('SIGTERM', onSignal);
    process.removeListener('SIGINT', onSignal);
    if (child) await terminateChild(child, 1000);
    await persist();
  }
}

if (path.resolve(process.argv[1] ?? '') === fileURLToPath(import.meta.url)) {
  runSupervisor(parseOptions()).then((state) => {
    const pass = state.selfTest ? state.selfTest.passed : true;
    process.exitCode = pass ? 0 : 1;
  }).catch((error) => {
    console.error(`[monitor-supervisor] ${error?.stack ?? error}`);
    process.exitCode = 1;
  });
}
