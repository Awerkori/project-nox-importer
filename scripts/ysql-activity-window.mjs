import fs from 'node:fs';
import pg from 'pg';
import dotenv from 'dotenv';
import { setTimeout as sleep } from 'node:timers/promises';

const envFile = process.env.NOX_AUDIT_ENV_FILE;
if (!envFile) throw new Error('NOX_AUDIT_ENV_FILE is required');

const minutes = Number(process.argv[2] || 3);
if (!Number.isInteger(minutes) || minutes < 0 || minutes > 10) throw new Error('Window must be 0..10 minutes');
const output = process.argv[3];

const env = dotenv.parse(fs.readFileSync(envFile));
const pool = new pg.Pool({
  host: env.YUGABYTE_HOST,
  port: Number(env.YUGABYTE_PORT || 5433),
  user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD,
  database: env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: true, ca: fs.readFileSync(env.YUGABYTE_SSL_CERT) },
  max: 1,
  connectionTimeoutMillis: 5000,
  query_timeout: 7000,
  application_name: 'nox-ysql-activity-audit',
  options: '-c statement_timeout=6000 -c default_transaction_read_only=on',
});

async function sample() {
  const [connections, heartbeat] = await Promise.all([
    pool.query(`
      SELECT
        COALESCE(NULLIF(application_name, ''), '(unset)') AS application_name,
        state,
        count(*)::int AS connections
      FROM pg_stat_activity
      WHERE datname = current_database()
      GROUP BY 1, 2
      ORDER BY 1, 2
    `),
    pool.query("SELECT created_at, data FROM importer_diagnostic_telemetry WHERE session_id = 'runtime'"),
  ]);
  const data = heartbeat.rows[0]?.data || {};
  return {
    at: new Date().toISOString(),
    connections: connections.rows,
    heartbeatAt: heartbeat.rows[0]?.created_at || null,
    eligible: data.queue?.eligible ?? null,
    importing: data.queue?.importing ?? null,
    effectiveConcurrency: data.effectiveConcurrency ?? null,
    dbPool: data.yugabyteDbPool ?? null,
  };
}

try {
  const end = Date.now() + minutes * 60_000;
  const samples = [];
  do {
    samples.push(await sample());
    if (Date.now() >= end) break;
    await sleep(Math.min(15_000, Math.max(0, end - Date.now())));
  } while (Date.now() < end);
  const report = { minutes, samples };
  if (output) fs.writeFileSync(output, JSON.stringify(report, null, 2), { mode: 0o600 });
  console.log(JSON.stringify(report, null, 2));
} finally {
  await pool.end();
}
