import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function run() {
  const res = await pool.query(`
    SELECT q.id, q.status, q.chapter_sort_key, (q.payload->>'workId') as work_id
    FROM importer_queue q
    WHERE q.source = 'montetai' AND q.priority = 100 AND q.status = 'QUEUED'
    LIMIT 5
  `);
  console.log('Priority 100 montetai samples:', res.rows);

  for (const row of res.rows) {
     const blockRes = await pool.query(`
       SELECT id, status, chapter_sort_key, attempts
       FROM importer_queue
       WHERE (payload->>'workId') = $1
         AND task_type = 'IMPORT_CHAPTER'
         AND chapter_sort_key < $2
         AND status IN ('QUEUED', 'RETRY', 'IMPORTING', 'FAILED', 'PAUSED_BY_STAFF')
     `, [row.work_id, row.chapter_sort_key]);
     console.log('Blocked by for', row.id, ':', blockRes.rows);
  }

  await pool.end();
}
run().catch(console.error);
