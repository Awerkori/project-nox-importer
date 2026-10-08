import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  const res = await pool.query(`SELECT data->>'schedulerAcquireBreakdown' as acquire, data->'runtimeFingerprint'->>'gitSha' as sha FROM importer_diagnostic_telemetry ORDER BY created_at DESC LIMIT 3`);
  console.log("Telemetry:", JSON.stringify(res.rows, null, 2));
  await pool.end();
}
main();
