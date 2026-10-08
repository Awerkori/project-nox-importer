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
        SELECT q.status, q.priority, q.payload->>'workId' as work_id
        FROM importer_queue q
        JOIN works w ON w.id::text = q.payload->>'workId' AND w.published IS TRUE
        WHERE q.task_type = 'IMPORT_CHAPTER'
          AND q.payload->>'workId' = '12da78a9-f771-4ea6-85cf-b193bd4a17a3'
          AND q.status IN ('QUEUED', 'RETRY')
          AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
  `);
  console.log("Queue items for 12da78a9:", res.rows);
  await pool.end();
}
run();
