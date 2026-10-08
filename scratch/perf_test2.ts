import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  const query = `
    EXPLAIN ANALYZE
    SELECT q.id
    FROM importer_queue q
    WHERE q.source = 'montetai'
      AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
      AND q.task_type = 'IMPORT_CHAPTER'
      AND q.attempts < COALESCE(q.max_attempts, 7)
    ORDER BY q.created_at ASC NULLS LAST, q.id ASC
    LIMIT 50
  `;
  try {
    const res = await pool.query(query);
    console.log(res.rows.map(r => r["QUERY PLAN"]).join("\n"));
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
main();
