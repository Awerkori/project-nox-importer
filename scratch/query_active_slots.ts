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
  const res = await pool.query(`SELECT payload->>'activeSlots' as active, payload->>'capacity' as cap FROM system_telemetry WHERE type = 'engine_heartbeat' ORDER BY created_at DESC LIMIT 1`);
  console.log("Slots:", res.rows);
  await pool.end();
}
main();
