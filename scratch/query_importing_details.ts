import { Pool } from 'pg';
import dotenv from 'dotenv';
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
    SELECT id, source, task_type, locked_by, 
           EXTRACT(EPOCH FROM (NOW() - locked_at)) as running_sec
    FROM importer_queue
    WHERE status = 'IMPORTING'
  `);
  console.table(res.rows);
  await pool.end();
}
run();
