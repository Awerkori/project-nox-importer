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
    SELECT q.priority, count(*) 
    FROM importer_queue q 
    JOIN importer_sources s ON q.source = s.id 
    WHERE q.status = 'QUEUED' AND q.task_type = 'IMPORT_CHAPTER' AND s.status = 'ACTIVE' AND s.blocked_reason IS NULL AND s.enabled = true
    GROUP BY q.priority
  `);
  console.log(res.rows);
  await pool.end();
}
run();
