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
  try {
    const res = await pool.query(`SELECT payload->>'activeSlots' as busy, payload->>'capacity' as cap FROM system_telemetry WHERE type = 'engine_heartbeat' ORDER BY created_at DESC LIMIT 5`);
    console.table(res.rows);
  } finally { await pool.end(); }
}
run();
