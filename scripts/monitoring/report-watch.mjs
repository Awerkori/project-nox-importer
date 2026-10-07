import path from 'node:path';
import process from 'node:process';
import { readJson, percentile } from './monitor-utils.mjs';

const args = process.argv.slice(2);
const stateDir = args[args.indexOf('--state-dir') + 1] || process.env.MONITOR_STATE_DIR || '/tmp/project-nox-vigilancia-9h-final';

function routeStats(snapshots, route) {
  const values = snapshots
    .filter((snapshot) => snapshot?.site?.samples?.[route]?.status === 'PASS')
    .map((snapshot) => snapshot?.site?.samples?.[route]?.durationMs)
    .filter((value) => Number.isFinite(value));
  return values.length ? { samples: values.length, p50: percentile(values, 0.5), p95: percentile(values, 0.95) } : { status: 'UNKNOWN', reason: 'no samples' };
}

const state = await readJson(path.join(stateDir, 'vigilancia-state.json'), null);
const result = await readJson(path.join(stateDir, 'result.json'), null);
if (!state) {
  console.error(`state not found: ${stateDir}`);
  process.exitCode = 1;
} else {
  const snapshots = [...(state.snapshots ?? []).filter(Boolean), result].filter(Boolean);
  const output = {
    originalWatchElapsedSeconds: state.originalWatchElapsedSeconds,
    effectiveMonitoringSeconds: Math.floor(state.effectiveSeconds),
    invalidHungSeconds: Math.floor(state.invalidSeconds),
    otherInvalidSeconds: Math.max(0, Math.floor(state.invalidSeconds - (state.incidents ?? []).filter((incident) => incident.type === 'preexisting_invalid_interval').reduce((sum, incident) => sum + Number(incident.seconds || 0), 0))),
    remainingEffectiveWatchSeconds: Math.max(0, Math.ceil(state.remainingEffectiveSeconds ?? state.targetEffectiveSeconds - state.effectiveSeconds)),
    currentStatus: state.currentStatus,
    supervisorPid: state.supervisorPid,
    task: state.task ?? null,
    lastCheckpointAt: state.lastCheckpointAt,
    lastProgressAt: state.lastProgressAt,
    importer: result?.importer ?? { status: 'UNKNOWN' },
    site: {
      status: result?.site?.status ?? 'UNKNOWN',
      home: routeStats(snapshots, 'home'),
      reader: routeStats(snapshots, 'reader'),
    },
    yugabyte: result?.yugabyte ?? { status: 'UNKNOWN' },
    telegram: result?.telegram ?? { status: 'UNKNOWN' },
    adaptive: {
      status: result?.adaptive?.status ?? 'UNKNOWN',
      scaleUp: result?.adaptive?.scaleUp ?? 0,
      scaleDown: result?.adaptive?.scaleDown ?? 0,
      recovery: result?.adaptive?.recovery ?? 0,
      observedEvents: state.adaptiveEvents?.length ?? 0,
    },
    incidents: state.incidents ?? [],
  };
  console.log(JSON.stringify(output, null, 2));
}
