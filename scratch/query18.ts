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
    SELECT q.id, q.status, q.chapter_sort_key, q.next_run_at
    FROM importer_queue q
    WHERE q.payload->>'workId' = 'b63c2720-61c8-4fe2-8469-c5550d57c645'
      AND q.task_type = 'IMPORT_CHAPTER'
    ORDER BY q.chapter_sort_key ASC
    LIMIT 10
  `);
  console.log('Black Clover jobs:', res.rows);

  await pool.end();
}
run().catch(console.error);
