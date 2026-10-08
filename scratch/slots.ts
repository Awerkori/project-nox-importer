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

  const res = await pool.query(`
    SELECT q.id, q.status, q.task_type, current_timestamp - q.updated_at as duration
    FROM importer_queue q
    WHERE q.status = 'IMPORTING'
  `);
  console.log("IMPORTING jobs:", res.rows);
  await pool.end();
}
run();
