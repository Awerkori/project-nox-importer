import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: {
    rejectUnauthorized: false
  }
});

async function run() {
  const res = await pool.query(`
    SELECT q.source, s.status, s.blocked_reason, COUNT(*) as cnt
    FROM importer_queue q
    LEFT JOIN importer_sources s ON q.source = s.id
    WHERE q.status IN ('QUEUED', 'RETRY') 
      AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
      AND q.task_type = 'IMPORT_CHAPTER'
    GROUP BY 1, 2, 3
    ORDER BY cnt DESC
  `);
  console.log(res.rows);
  await pool.end();
}
run().catch(console.error);
