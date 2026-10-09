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
      SELECT 
        COUNT(*) as total_works,
        COUNT(*) FILTER (WHERE min_sort_key <= 1.5) as works_with_valid_frontier,
        COUNT(*) FILTER (WHERE min_sort_key > 1.5) as works_blocked_by_frontier
      FROM (
        SELECT q.payload->>'workId' as work_id, MIN(q.chapter_sort_key) as min_sort_key
        FROM importer_queue q
        LEFT JOIN chapters c ON c.work_id = (q.payload->>'workId')::uuid AND c.published_at IS NOT NULL
        WHERE q.status = 'QUEUED' AND q.task_type = 'IMPORT_CHAPTER' AND c.id IS NULL
        GROUP BY q.payload->>'workId'
      ) t
    `);
    console.log(JSON.stringify(res.rows, null, 2));
  } catch (err) {
    console.error(err.message);
  }
  await pool.end();
}
run();
