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
    SELECT q.id, q.status, q.chapter_sort_key, s.status as source_status
    FROM importer_queue q
    LEFT JOIN importer_sources s ON q.source = s.id
    WHERE q.payload->>'workId' = '0f43aad4-459f-4fb5-be3a-a5e034e36487'
      AND q.task_type = 'IMPORT_CHAPTER'
    ORDER BY q.chapter_sort_key ASC
    LIMIT 20
  `);
  console.log("Predecessors:", res.rows);
  await pool.end();
}
run();
