import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import process from 'node:process';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import { runInvalidSqlConnectionCloseTest, runQueryTimeoutTest } from './postgres-probe.mjs';
import { withTimeout } from './monitor-utils.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const supervisor = path.join(here, 'monitor-supervisor.mjs');
async function runExternalWatchdogTest() {
  const stateDir = await fs.mkdtemp(path.join(os.tmpdir(), 'nox-watchdog-self-test-'));
  const child = spawn(process.execPath, [supervisor, '--self-test', '--state-dir', stateDir, '--warning-ms', '100', '--kill-ms', '250', '--worker-deadline-ms', '500', '--checkpoint-ms', '50', '--stale-checkpoint-ms', '200', '--tick-ms', '25'], { stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (data) => { stdout += String(data); });
  child.stderr.on('data', (data) => { stderr += String(data); });
  const exit = await withTimeout(new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal }))), 5000, 'external watchdog self-test');
  const state = JSON.parse(await fs.readFile(path.join(stateDir, 'vigilancia-state.json'), 'utf8'));
  await fs.rm(stateDir, { recursive: true, force: true });
  return { passed: exit.code === 0 && state.selfTest?.passed === true && state.incidents.some((incident) => incident.type === 'task_killed'), exit, state, stdout, stderr };
}

async function runLocalSqlEngineTest() {
  let db;
  let queryError;
  let closed = false;
  const started = Date.now();
  try {
    db = new PGlite();
    await withTimeout(db.waitReady, 2000, 'PGlite startup');
    try {
      await withTimeout(db.query('SELECT coluna_inexistente;'), 500, 'invalid SQL query');
    } catch (error) {
      queryError = error;
    }
  } finally {
    if (db) {
      await withTimeout(db.close(), 1000, 'PGlite close').then(() => { closed = true; }).catch(() => {});
    }
  }
  return {
    passed: Boolean(queryError && closed),
    queryError: queryError ? String(queryError.message ?? queryError) : null,
    closed,
    durationMs: Date.now() - started,
  };
}

async function main() {
  const invalidSql = await runInvalidSqlConnectionCloseTest({
    clientFactory: () => {
      let ended = false;
      return {
        async connect() {},
        async query() { const error = new Error('column "coluna_inexistente" does not exist'); error.code = '42703'; throw error; },
        async end() { ended = true; },
        get ended() { return ended; },
      };
    },
    timeouts: { connectionMs: 100, externalMs: 500, closeMs: 100 },
  });
  const localSqlEngine = await runLocalSqlEngineTest();
  const queryTimeout = await runQueryTimeoutTest({ queryTimeoutMs: 50, externalMs: 300, timeouts: { connectionMs: 100, closeMs: 100 } });
  const watchdog = await runExternalWatchdogTest();
  const result = { invalidSql, localSqlEngine, queryTimeout, watchdog };
  console.log(JSON.stringify(result, null, 2));
  process.exitCode = invalidSql.passed && localSqlEngine.passed && queryTimeout.passed && watchdog.passed ? 0 : 1;
}

main().catch((error) => {
  console.error(error?.stack ?? error);
  process.exitCode = 1;
});
