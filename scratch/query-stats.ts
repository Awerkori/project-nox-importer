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
    SELECT status, COUNT(*)
    FROM importer_queue
    WHERE task_type = 'IMPORT_CHAPTER'
    GROUP BY status
    ORDER BY count DESC;
  `);
  console.log("Queue Status:");
  console.table(res.rows);

  const chaptersRes = await pool.query(`
    SELECT COUNT(*)
    FROM chapters
    WHERE published_at >= NOW() - INTERVAL '5 minutes';
  `);
  console.log("Chapters published in last 5m:", chaptersRes.rows[0].count);

  await pool.end();
}
run().catch(console.error);
