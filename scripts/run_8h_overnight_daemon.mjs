// Compatibility entry point. The previous daemon kept one PostgreSQL client
// and one event loop alive for hours without an external deadline. Vigilia now
// runs bounded worker cycles under an independent supervisor and persists its
// effective-time state atomically.
import path from 'node:path';
import { runSupervisor } from './monitoring/monitor-supervisor.mjs';

const stateDir = process.env.MONITOR_STATE_DIR ?? '/tmp/project-nox-vigilancia';
const state = await runSupervisor({
  stateDir,
  stateFile: path.join(stateDir, 'vigilancia-state.json'),
  heartbeatFile: path.join(stateDir, 'heartbeat.json'),
  resultFile: path.join(stateDir, 'result.json'),
  targetEffectiveSeconds: 9 * 60 * 60,
});

process.exitCode = state.currentStatus === 'completed' ? 0 : 1;

