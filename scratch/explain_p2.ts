import { Pool } from 'pg';
import dotenv from 'dotenv';
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
    EXPLAIN WITH eligible_sources AS MATERIALIZED (
      SELECT id FROM importer_sources WHERE enabled = true
    ), source_window AS MATERIALIZED (
      SELECT q.*
      FROM importer_queue q
      JOIN eligible_sources s ON s.id = q.source
      WHERE q.task_type = 'IMPORT_CHAPTER'
        AND (q.status = 'QUEUED' OR q.status = 'PAUSED_BY_STAFF')
        AND q.priority >= 50 AND q.priority < 75
      ORDER BY q.priority DESC, q.chapter_sort_key ASC
      LIMIT 160
    )
    SELECT * FROM source_window;
  `;
  const res = await pool.query(query);
  console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  await pool.end();
}
run().catch(console.error);
