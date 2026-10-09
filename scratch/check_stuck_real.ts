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
    SELECT work_id, min(chapter_sort_key) as min_staged
    FROM importer_chapter_mappings
    WHERE status IN ('STAGED', 'WAITING_FOR_GAP')
      AND work_id IS NOT NULL
    GROUP BY work_id
    LIMIT 10
  `);
  
  for (const row of res.rows) {
      console.log(row);
      const gaps = await pool.query(`SELECT start_sort_key, end_sort_key FROM importer_confirmed_gaps WHERE work_id = $1`, [row.work_id]);
      console.log("gaps:", gaps.rows);
      
      const queued = await pool.query(`SELECT status, task_type, priority, next_run_at FROM importer_queue WHERE payload->>'workId' = $1 AND chapter_sort_key < $2`, [row.work_id, row.min_staged]);
      console.log("queued predecessors:", queued.rows);
  }

  await pool.end();
}
run();
