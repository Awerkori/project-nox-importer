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
    SELECT q.*
    FROM importer_queue q
    WHERE q.payload->>'workId' = '0f43aad4-459f-4fb5-be3a-a5e034e36487'
      AND q.task_type = 'IMPORT_CHAPTER'
      AND q.status IN ('QUEUED', 'RETRY')
    ORDER BY q.chapter_sort_key ASC
    LIMIT 10
  `);
  console.log("Queue entries for work:", res.rows.map(r => ({id: r.id, sort_key: r.chapter_sort_key, status: r.status})));

  const res2 = await pool.query(`
    SELECT number, published_at FROM chapters WHERE work_id = '0f43aad4-459f-4fb5-be3a-a5e034e36487' ORDER BY number ASC
  `);
  console.log("Chapters for work:", res2.rows.length, "chapters. Latest published:", res2.rows.filter(r => r.published_at).map(r => r.number).pop());

  await pool.end();
}
run();
