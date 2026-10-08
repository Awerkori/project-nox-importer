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
    SELECT COUNT(*) 
    FROM importer_queue q
    WHERE q.status IN ('QUEUED', 'RETRY') 
      AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
      AND q.task_type = 'IMPORT_CHAPTER'
      AND q.priority >= 50
      AND q.attempts >= COALESCE(q.max_attempts, 7)
  `);
  console.log('Jobs with exhausted attempts:', res.rows[0]);

  await pool.end();
}
run().catch(console.error);
