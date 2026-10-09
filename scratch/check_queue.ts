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
    const res = await pool.query(`
      SELECT COUNT(*) as eligible FROM importer_queue
      WHERE status = 'QUEUED' AND (next_run_at IS NULL OR next_run_at <= NOW());
    `);
    console.log("Eligible jobs:", res.rows[0]);
  } catch (err) {
    console.error(err.message);
  }
  await pool.end();
}
run();
