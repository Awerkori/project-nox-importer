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
      SELECT id, payload->>'workId' as work_id, payload->>'chapterNumber' as number, source, status, updated_at
      FROM importer_queue
      WHERE status = 'IMPORTING'
      ORDER BY updated_at ASC
    `);
    console.log(res.rows);
  } catch (err) {
    console.error(err);
  }
  await pool.end();
}
run();
