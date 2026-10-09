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
      SELECT w.published, q.priority, count(*) as cnt
      FROM importer_queue q
      LEFT JOIN works w ON w.id = (q.payload->>'workId')::uuid
      WHERE q.status = 'QUEUED' AND q.task_type = 'IMPORT_CHAPTER' AND q.priority < 100
      GROUP BY w.published, q.priority
    `);
    console.log(JSON.stringify(res.rows, null, 2));
  } catch (err) {
    console.error(err.message);
  }
  await pool.end();
}
run();
