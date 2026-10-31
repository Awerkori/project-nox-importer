import { describe, expect, it } from 'vitest';
import { probeYugabyte, makeClientConfig, runInvalidSqlConnectionCloseTest, runQueryTimeoutTest } from '../scripts/monitoring/postgres-probe.mjs';
import { runSupervisor } from '../scripts/monitoring/monitor-supervisor.mjs';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

describe('monitoramento anti-loop bounded e externo', () => {
  const env = { YUGABYTE_HOST: 'fixture', YUGABYTE_USER: 'fixture', YUGABYTE_PASSWORD: 'fixture', YUGABYTE_DATABASE: 'fixture' };
  function fixture(freshnessFails = false) {
    const queries = [];
    let closed = false;
    return {
      queries, get closed() { return closed; },
      clientFactory: () => ({
        async connect() {}, async end() { closed = true; },
        async query({ text }) {
          queries.push(text);
          if (text.includes('pg_stat_activity')) return { rows: [{ total: 3, active: 1, max_connections: 100 }] };
          if (text.includes('FROM chapters')) {
            if (freshnessFails) throw new Error('publication probe unavailable');
            return { rows: [{ fresh_5m: 20, fresh_30m: 120, last_fresh: new Date().toISOString() }] };
          }
          if (text.includes('FROM importer_lite_tasks')) return { rows: [{ queue_debt: 40, ready_work_candidates: 5, current_capacity: 2, expired_leases: 1, monitors: 10, overdue_monitors: 4 }] };
          if (text.includes('FROM settings')) return { rows: [] };
          throw new Error('Unexpected probe');
        },
      }),
    };
  }

  it('observa filas Lite e distingue candidatos de trabalho efetivamente executável', async () => {
    const f = fixture();
    const result = await probeYugabyte({ env, clientFactory: f.clientFactory });
    expect(result.status).toBe('PASS');
    expect(result.importer).toMatchObject({ runtime: 'lite', queueDebt: 40, readyWorkCandidates: 5, effectiveClaimable: null, staged: null, overdueMonitors: 4, expiredLeases: 1, readerPagesVerified: false });
    expect(f.queries.join('\n')).not.toContain('FROM importer_queue');
    expect(f.closed).toBe(true);
    expect(makeClientConfig(env).options).toContain('default_transaction_read_only=on');
  });

  it('não transforma falha na medição de publicação em PASS quando a fila responde', async () => {
    const f = fixture(true);
    const result = await probeYugabyte({ env, clientFactory: f.clientFactory });
    expect(result.status).toBe('UNKNOWN');
    expect(result.importer.status).toBe('UNKNOWN');
    expect(result.importer.freshLast5m).toBe(null);
    expect(result.importer.queueDebt).toBe(40);
    expect(f.closed).toBe(true);
  });

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
