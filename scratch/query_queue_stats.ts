import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
});

async function main() {
  const res = await pool.query(`
    SELECT status, task_type, COUNT(*) as count 
    FROM importer_queue 
    GROUP BY status, task_type 
    ORDER BY status, count DESC;
  `);
  console.table(res.rows);
  
  const eligible = await pool.query(`
    SELECT COUNT(*) as eligible_count
    FROM importer_queue jq
    WHERE jq.status IN ('QUEUED', 'RETRY')
      AND jq.next_run_at <= NOW()
  `);
  console.log("Eligible Jobs:", eligible.rows[0].eligible_count);
  
  const jobs = await pool.query(`
    SELECT task_type, COUNT(*) as c
    FROM importer_queue jq
    WHERE jq.status IN ('QUEUED', 'RETRY')
      AND jq.next_run_at <= NOW()
    GROUP BY task_type
  `);
  console.table(jobs.rows);
  
  pool.end();
}
main().catch(console.error);
