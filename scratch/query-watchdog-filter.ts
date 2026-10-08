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
  const query = `
    SELECT COUNT(*) as eligible_cnt
    FROM importer_queue q
    JOIN importer_sources s ON s.id = q.source
    WHERE q.status IN ('QUEUED', 'RETRY')
      AND q.priority >= 50
      AND q.published_at IS NULL
      AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
      AND s.status = 'ACTIVE'
      AND q.task_type = 'IMPORT_CHAPTER'
      AND NOT EXISTS (
        SELECT 1
        FROM importer_queue p
        WHERE p.canonical_url = q.canonical_url
          AND p.sequence_number < q.sequence_number
          AND p.status IN ('QUEUED', 'RETRY', 'IMPORTING')
          AND p.published_at IS NULL
      )
  `;
  const res = await pool.query(query);
  console.log("Eligible count with filter:", res.rows[0].eligible_cnt);
  await pool.end();
}
run().catch(console.error);
