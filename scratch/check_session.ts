import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();
async function run() {
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });
  const res = await pool.query(`SELECT id, session_id, created_at, data->'runtimeFingerprint'->>'workerId' as worker_id, data->'runtimeFingerprint'->>'pid' as pid, data->'runtimeFingerprint'->>'gitSha' as sha FROM importer_diagnostic_telemetry ORDER BY created_at DESC LIMIT 5`);
  console.log(res.rows);
  await pool.end();
}
run();
