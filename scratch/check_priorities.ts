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
      SELECT priority, task_type, COUNT(*) as cnt
      FROM importer_queue
      WHERE status = 'QUEUED'
      GROUP BY priority, task_type
      ORDER BY priority DESC
    `);
    console.log(JSON.stringify(res.rows, null, 2));
  } catch (err) {
    console.error(err.message);
  }
  await pool.end();
}
run();
