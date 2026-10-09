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

  const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
  console.log("Heartbeat:", res.rows[0]?.value);
  
  const tel = await pool.query("SELECT created_at, data->'runtimeFingerprint'->>'gitSha' as sha FROM importer_diagnostic_telemetry ORDER BY created_at DESC LIMIT 3");
  console.log("Telemetry updates:", tel.rows);

  await pool.end();
}
run();
