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
  const res = await pool.query(`SELECT id, status, source, last_error, attempts, updated_at FROM importer_queue WHERE payload->>'workId' = '90ea547a-f7c5-4d63-9dfb-10cc480c3fdc' ORDER BY chapter_sort_key ASC LIMIT 5`);
  console.log("Jobs:", res.rows);
  await pool.end();
}
main();
