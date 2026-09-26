import { describe, expect, it } from 'vitest';
import { runInvalidSqlConnectionCloseTest, runQueryTimeoutTest } from '../scripts/monitoring/postgres-probe.mjs';
import { runSupervisor } from '../scripts/monitoring/monitor-supervisor.mjs';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

describe('monitoramento anti-loop bounded e externo', () => {
  it('fecha a conexão mesmo quando SQL inválido retorna erro', async () => {
    const result = await runInvalidSqlConnectionCloseTest({
      clientFactory: () => ({
        async connect() {},
        async query() { throw Object.assign(new Error('column does not exist'), { code: '42703' }); },
        async end() {},
      }),
      timeouts: { connectionMs: 100, externalMs: 300, closeMs: 100 },
    });
    expect(result.passed).toBe(true);
    expect(result.closed).toBe(true);
    expect(result.durationMs).toBeLessThan(1000);
  });

  it('encerra query que não responde e fecha o cliente', async () => {
    const result = await runQueryTimeoutTest({ queryTimeoutMs: 30, externalMs: 200, timeouts: { connectionMs: 100, closeMs: 100 } });
    expect(result.passed).toBe(true);
    expect(result.closed).toBe(true);
    expect(result.durationMs).toBeLessThan(1000);
  });

  it('supervisor externo mata worker pendurado, registra incidente e continua até checkpoint', async () => {
    const stateDir = await fs.mkdtemp(path.join(os.tmpdir(), 'nox-watchdog-vitest-'));
    const state = await runSupervisor({
      selfTest: true,
      stateDir,
      stateFile: path.join(stateDir, 'state.json'),
      heartbeatFile: path.join(stateDir, 'heartbeat.json'),
      resultFile: path.join(stateDir, 'result.json'),
      warningMs: 30,
      killMs: 80,
      workerDeadlineMs: 120,
      checkpointMs: 20,
      staleCheckpointMs: 100,
      tickMs: 10,
      cycleDelayMs: 10,
      targetEffectiveSeconds: 1,
    });
    expect(state.selfTest?.passed).toBe(true);
    expect(state.incidents.some((incident) => incident.type === 'task_killed')).toBe(true);
    expect(state.checkpoints.length).toBeGreaterThan(0);
    await fs.rm(stateDir, { recursive: true, force: true });
  });
});
