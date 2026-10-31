import fs from 'node:fs';
import path from 'node:path';
import pg from 'pg';
import dotenv from 'dotenv';
import { withTimeout, monotonicMs, parseJsonValue } from './monitor-utils.mjs';

dotenv.config({ path: process.env.PROJECT_NOX_ENV_FILE ?? path.resolve(process.cwd(), '.env') });

export const DB_TIMEOUTS = Object.freeze({
  connectionMs: 5000,
  statementMs: 5000,
  queryMs: 6000,
  closeMs: 1500,
  externalMs: 10000,
});

function sslConfig() {
  const certPath = process.env.YUGABYTE_SSL_CERT;
  if (certPath && fs.existsSync(certPath)) {
    return { rejectUnauthorized: true, ca: fs.readFileSync(certPath, 'utf8') };
  }
  // Keep compatibility with the existing environment. The monitor never
  // prints credentials and uses a short-lived read-only client per cycle.
  return { rejectUnauthorized: false };
}

export function hasDbConfig(env = process.env) {
  return Boolean(env.YUGABYTE_HOST && env.YUGABYTE_USER && env.YUGABYTE_PASSWORD && env.YUGABYTE_DATABASE);
}

export function makeClientConfig(env = process.env, overrides = {}) {
  return {
    host: env.YUGABYTE_HOST,
    port: Number(env.YUGABYTE_PORT || 5433),
    user: env.YUGABYTE_USER,
    password: env.YUGABYTE_PASSWORD,
    database: env.YUGABYTE_DATABASE,
    ssl: sslConfig(),
    application_name: 'project-nox-vigilancia',
    connectionTimeoutMillis: overrides.connectionTimeoutMillis ?? DB_TIMEOUTS.connectionMs,
    statement_timeout: overrides.statement_timeout ?? DB_TIMEOUTS.statementMs,
    query_timeout: overrides.query_timeout ?? DB_TIMEOUTS.queryMs,
    idle_in_transaction_session_timeout: overrides.idle_in_transaction_session_timeout ?? DB_TIMEOUTS.statementMs,
    options: `-c default_transaction_read_only=on -c statement_timeout=${overrides.statement_timeout ?? DB_TIMEOUTS.statementMs} -c idle_in_transaction_session_timeout=${overrides.idle_in_transaction_session_timeout ?? DB_TIMEOUTS.statementMs}`,
  };
}

async function closeClient(client, closeMs = DB_TIMEOUTS.closeMs) {
  if (!client) return { closed: true, forced: false };
  try {
    await withTimeout(client.end(), closeMs, 'postgres connection close');
    return { closed: true, forced: false };
  } catch (error) {
    // pg exposes the underlying stream on the client. Destroying it is the
    // final bounded cleanup when end() itself is waiting on a broken socket.
    const stream = client.connection?.stream;
    if (stream && !stream.destroyed) stream.destroy();
    return { closed: false, forced: true, error: String(error?.message ?? error) };
  }
}

export async function withPostgresClient(operation, options = {}) {
  const timeouts = { ...DB_TIMEOUTS, ...(options.timeouts ?? {}) };
  const clientFactory = options.clientFactory ?? (() => new pg.Client(makeClientConfig(options.env ?? process.env, timeouts)));
  let client;
  let closeResult = { closed: true, forced: false };
  const started = monotonicMs();

  try {
    client = clientFactory();
    await withTimeout(client.connect(), timeouts.connectionMs, 'postgres connection');
    return await withTimeout(operation(client), timeouts.externalMs, 'postgres operation');
  } finally {
    if (client) {
      closeResult = await closeClient(client, timeouts.closeMs);
      if (options.onClose) options.onClose(closeResult);
    }
    if (options.onDuration) options.onDuration(Math.round(monotonicMs() - started));
  }
}

export async function queryWithTimeout(client, text, values = [], options = {}) {
  const timeoutMs = options.timeoutMs ?? DB_TIMEOUTS.queryMs;
  return withTimeout(
    client.query({ text, values }),
    timeoutMs,
    options.label ?? 'postgres query',
  );
}

async function scalarQuery(client, text, values = []) {
  const result = await queryWithTimeout(client, text, values);
  return result.rows?.[0] ?? {};
}

export async function probeYugabyte(options = {}) {
  if (!hasDbConfig(options.env ?? process.env)) {
    return { status: 'UNKNOWN', reason: 'YUGABYTE_* configuration unavailable' };
  }

  const started = monotonicMs();
  try {
    return await withPostgresClient(async (client) => {
      let freshnessOk = false;
      let queueOk = false;
      const result = {
        status: 'PASS',
        durationMs: null,
        connections: null,
        active: null,
        idle: null,
        idleInTransaction: null,
        slowQueries: null,
        maxConnections: null,
        poolPressure: null,
        saturation: null,
        importer: {
          status: 'UNKNOWN',
          freshLast5m: null,
          freshLast30m: null,
          currentCapacity: null,
          queueDebt: null,
          staged: null,
          expiredLeases: null,
          effectiveClaimable: null,
          releaseLagSeconds: null,
          lastFresh: null,
        },
        adaptive: { status: 'UNKNOWN', events: [] },
      };

      try {
        const activity = await scalarQuery(client, `
          SELECT COUNT(*)::int AS total,
                 COUNT(*) FILTER (WHERE state = 'active')::int AS active,
                 COUNT(*) FILTER (WHERE state = 'idle')::int AS idle,
                 COUNT(*) FILTER (WHERE state = 'idle in transaction')::int AS idle_in_transaction,
                 COUNT(*) FILTER (WHERE state = 'active' AND query_start < NOW() - INTERVAL '5 seconds')::int AS slow_queries,
                 current_setting('max_connections')::int AS max_connections
          FROM pg_stat_activity WHERE datname = current_database()
        `);
        result.connections = Number(activity.total ?? 0);
        result.active = Number(activity.active ?? 0);
        result.idle = Number(activity.idle ?? 0);
        result.idleInTransaction = Number(activity.idle_in_transaction ?? 0);
        result.slowQueries = Number(activity.slow_queries ?? 0);
        result.maxConnections = Number(activity.max_connections ?? 0);
        result.poolPressure = result.maxConnections > 0 ? Number((result.active / result.maxConnections).toFixed(4)) : null;
        result.saturation = result.poolPressure;
      } catch (error) {
        result.connections = { status: 'UNKNOWN', reason: String(error?.message ?? error) };
      }

      try {
        const freshness = await scalarQuery(client, `
          SELECT MAX(c.published_at) AS last_fresh,
                 COUNT(*) FILTER (WHERE c.published_at >= NOW() - INTERVAL '5 minutes')::int AS fresh_5m,
                 COUNT(*)::int AS fresh_30m
          FROM chapters c JOIN works w ON w.id=c.work_id
          WHERE c.published_at >= NOW() - INTERVAL '30 minutes' AND w.published=true
        `);
        freshnessOk = true;
        result.importer.publicationEvidence = 'SQL_PUBLIC_VISIBILITY_ONLY';
        result.importer.readerPagesVerified = false;
        result.importer.lastFresh = freshness.last_fresh ?? null;
        result.importer.freshLast5m = Number(freshness.fresh_5m ?? 0);
        result.importer.freshLast30m = Number(freshness.fresh_30m ?? 0);
        result.importer.releaseLagSeconds = freshness.last_fresh ? Math.max(0, Math.round((Date.now() - Date.parse(freshness.last_fresh)) / 1000)) : null;
      } catch (error) {
        result.importer.status = 'UNKNOWN';
        result.importer.reason = String(error?.message ?? error);
      }

      try {
        const queue = await scalarQuery(client, `
          SELECT COUNT(*) FILTER (WHERE state IN ('READY','RETRY_WAIT') AND monitoring=false)::int AS queue_debt,
                 (SELECT count(*)::int FROM importer_lite_ready_works WHERE next_run_at<=now()) AS ready_work_candidates,
                 COUNT(*) FILTER (WHERE state='RUNNING' AND task_type='IMPORT_CHAPTER')::int AS current_capacity,
                 COUNT(*) FILTER (WHERE state='READY' AND task_type='PUBLISH')::int AS pending_publish_tasks,
                 COUNT(*) FILTER (WHERE state='RUNNING' AND lease_until<now())::int AS expired_leases,
                 COUNT(*) FILTER (WHERE monitoring AND task_type='CATALOG' AND state IN ('READY','RETRY_WAIT','RUNNING'))::int AS monitors,
                 COUNT(*) FILTER (WHERE monitoring AND task_type='CATALOG' AND state IN ('READY','RETRY_WAIT') AND next_run_at<now())::int AS overdue_monitors,
                 MIN(next_run_at) FILTER (WHERE monitoring AND task_type='CATALOG' AND state IN ('READY','RETRY_WAIT')) AS earliest_monitor_due
          FROM importer_lite_tasks WHERE state IN ('READY','RETRY_WAIT','RUNNING')
        `);
        result.importer.queueDebt = Number(queue.queue_debt ?? 0);
        queueOk = true;
        result.importer.runtime = 'lite';
        // Projection candidates may still be blocked by source health or barriers.
        result.importer.readyWorkCandidates = Number(queue.ready_work_candidates ?? 0);
        result.importer.monitors = Number(queue.monitors ?? 0);
        result.importer.overdueMonitors = Number(queue.overdue_monitors ?? 0);
        result.importer.earliestMonitorDue = queue.earliest_monitor_due ?? null;
        result.importer.currentCapacity = Number(queue.current_capacity ?? 0);
        // Ready PUBLISH tasks are not proof of stored media or of all unpublished receipts.
        result.importer.pendingPublishTasks = Number(queue.pending_publish_tasks ?? 0);
        result.importer.expiredLeases = Number(queue.expired_leases ?? 0);
        result.importer.status = freshnessOk && queueOk ? 'PASS' : 'UNKNOWN';
      } catch (error) {
        result.importer.status = 'UNKNOWN';
        result.importer.reason = String(error?.message ?? error);
      }

      try {
        const settings = await queryWithTimeout(client, `
          SELECT key, value FROM settings
          WHERE key IN ('importer_heartbeat', 'autotuner_state', 'adaptive_controller_state', 'capacity_controller_state')
        `);
        const byKey = Object.fromEntries(settings.rows.map((row) => [row.key, parseJsonValue(row.value)]));
        const adaptive = byKey.autotuner_state ?? byKey.adaptive_controller_state ?? byKey.capacity_controller_state;
        if (adaptive !== undefined) {
          result.adaptive = { status: 'PASS', state: adaptive, events: Array.isArray(adaptive?.events) ? adaptive.events : [] };
        }
        const heartbeat = byKey.importer_heartbeat;
        if (heartbeat && typeof heartbeat === 'object') result.importer.heartbeat = heartbeat;
      } catch (error) {
        result.adaptive = { status: 'UNKNOWN', reason: String(error?.message ?? error), events: [] };
      }

      if (!freshnessOk || !queueOk || typeof result.connections !== 'number') result.status = 'UNKNOWN';
      result.durationMs = Math.round(monotonicMs() - started);
      return result;
    }, options);
  } catch (error) {
    return {
      status: 'UNKNOWN',
      reason: String(error?.message ?? error),
      durationMs: Math.round(monotonicMs() - started),
      importer: { status: 'UNKNOWN' },
      adaptive: { status: 'UNKNOWN', events: [] },
    };
  }
}

export async function runInvalidSqlConnectionCloseTest(options = {}) {
  let closeResult;
  let queryError;
  const started = monotonicMs();
  try {
    await withPostgresClient(async (client) => {
      try {
        await queryWithTimeout(client, 'SELECT coluna_inexistente;', [], { timeoutMs: 1000, label: 'invalid SQL test' });
      } catch (error) {
        queryError = error;
      }
      if (!queryError) throw new Error('invalid SQL unexpectedly succeeded');
    }, { ...options, onClose: (result) => { closeResult = result; } });
  } catch (error) {
    if (!queryError) queryError = error;
  }
  return {
    passed: Boolean(queryError && closeResult?.closed),
    queryError: queryError ? String(queryError.message ?? queryError) : null,
    closed: Boolean(closeResult?.closed),
    forcedClose: Boolean(closeResult?.forced),
    durationMs: Math.round(monotonicMs() - started),
  };
}

export async function runQueryTimeoutTest(options = {}) {
  let closeResult;
  let timeoutError;
  const started = monotonicMs();
  const clientFactory = options.clientFactory ?? (() => ({
    async connect() {},
    query() { return new Promise(() => {}); },
    async end() {},
  }));
  try {
    await withPostgresClient(async (client) => {
      try {
        await queryWithTimeout(client, 'SELECT pg_sleep(999);', [], { timeoutMs: options.queryTimeoutMs ?? 50, label: 'query timeout test' });
      } catch (error) {
        timeoutError = error;
      }
      if (!timeoutError) throw new Error('query timeout test unexpectedly completed');
    }, {
      ...options,
      clientFactory,
      timeouts: { ...options.timeouts, externalMs: options.externalMs ?? 500 },
      onClose: (result) => { closeResult = result; },
    });
  } catch (error) {
    if (!timeoutError) timeoutError = error;
  }
  return {
    passed: Boolean(timeoutError?.code === 'MONITOR_TIMEOUT' && closeResult?.closed),
    timeoutError: timeoutError ? String(timeoutError.message ?? timeoutError) : null,
    closed: Boolean(closeResult?.closed),
    durationMs: Math.round(monotonicMs() - started),
  };
}
