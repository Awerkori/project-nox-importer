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
      SELECT payload->>'workId' as work_id, count(*)
      FROM importer_queue
      WHERE status = 'QUEUED'
      GROUP BY payload->>'workId'
      ORDER BY count(*) DESC
      LIMIT 10
    `);
    console.table(res.rows);
  } finally { await pool.end(); }
}
run();
