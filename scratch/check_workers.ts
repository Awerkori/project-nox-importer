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
      SELECT (value::jsonb)->>'id' as worker_id, (value::jsonb)->>'status' as status
      FROM settings 
      WHERE key = 'importer_heartbeat';
    `);
    console.table(res.rows);
  } catch (err) {
    console.error(err.message);
  }
  await pool.end();
}
run();
