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

  const query = `
      WITH q_candidates AS (
        SELECT q.id, q.payload, q.chapter_sort_key, q.next_run_at, q.priority
        FROM importer_queue q
        WHERE (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
          AND q.task_type = 'IMPORT_CHAPTER'
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND (
            q.priority >= 1000
            OR (q.payload->>'workId') = ANY($1::text[])
          )
      )
      SELECT qc.id
      FROM q_candidates qc
      JOIN importer_sources s ON s.id = qc.payload->>'source'
  `;
  try {
    console.time("Query");
    await pool.query(query, [[]]);
    console.timeEnd("Query");
  } catch (err) {
    console.error(err);
  }
  await pool.end();
}
run();
