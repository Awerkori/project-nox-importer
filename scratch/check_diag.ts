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
  const res = await pool.query(`SELECT data->>'status' as status, data->>'noProgressReason' as reason, created_at, data->>'throughput' as tp, data->>'capacity' as cap FROM importer_diagnostic_telemetry ORDER BY created_at DESC LIMIT 1`);
  console.log(res.rows[0]);
  await pool.end();
}
run();
