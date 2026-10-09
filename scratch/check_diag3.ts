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

  const res = await pool.query("SELECT * FROM importer_diagnostic_telemetry WHERE data->'schedulerAcquireBreakdown' IS NOT NULL AND data->'schedulerAcquireBreakdown'->'totalMs'->>'avg' != '0' ORDER BY created_at DESC LIMIT 1");
  if (res.rows.length > 0) console.log(JSON.stringify(res.rows[0].data.schedulerAcquireBreakdown, null, 2));
  else console.log("No non-zero breakdown found");
  await pool.end();
}
run();
