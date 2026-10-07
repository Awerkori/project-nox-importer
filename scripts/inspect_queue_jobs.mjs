import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  const res = await pool.query(`
    SELECT q.id, q.source, (q.payload->>'workId') as work_id, q.chapter_sort_key, q.priority, q.status, q.locked_by, q.locked_at
    FROM importer_queue q
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND q.status IN ('QUEUED', 'IMPORTING')
    ORDER BY q.priority DESC, q.chapter_sort_key ASC
    LIMIT 20;
  `);
  console.log("Top 20 queued/importing jobs:");
  console.table(res.rows);

  const impRes = await pool.query(`
    SELECT q.id, q.source, (q.payload->>'workId') as work_id, q.chapter_sort_key, q.priority, q.locked_by, q.locked_at
    FROM importer_queue q
    WHERE q.task_type = 'IMPORT_CHAPTER' AND q.status = 'IMPORTING';
  `);
  console.log("\nCurrently IMPORTING jobs:", impRes.rows.length);
  console.table(impRes.rows);

  await pool.end();
}

main().catch(console.error);
