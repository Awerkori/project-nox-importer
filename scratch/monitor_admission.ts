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

  console.log("Checking job_admission telemetry...");
  try {
    const res = await pool.query(`
      SELECT created_at, payload->>'status' as status, payload->>'lane' as lane, 
             payload->>'claim_sql_ms' as claim_sql_ms, payload->>'claim_lock_sql_exec_ms' as claim_lock_sql_exec_ms
      FROM system_telemetry 
      WHERE type = 'job_admission' 
      ORDER BY created_at DESC 
      LIMIT 10
    `);
    console.table(res.rows);
  } catch (err) {
    console.error(err.message);
  }
  await pool.end();
}
run();
