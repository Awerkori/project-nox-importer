import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
});

async function main() {
  const workId = '72e5145a-151f-4b0c-92ed-b5de358f253f';

  // chapters
  const ch = await pool.query(`SELECT number, published_at FROM chapters WHERE work_id = $1 ORDER BY number DESC LIMIT 5`, [workId]);
  console.log("Chapters:");
  console.table(ch.rows);

  // mappings
  const map = await pool.query(`SELECT chapter_sort_key, status, is_gap FROM importer_chapter_mappings WHERE work_id = $1 ORDER BY chapter_sort_key DESC LIMIT 5`, [workId]);
  console.log("Mappings:");
  console.table(map.rows);

  // queue
  const q = await pool.query(`SELECT id, chapter_sort_key, status, priority FROM importer_queue WHERE payload->>'workId' = $1 AND task_type = 'IMPORT_CHAPTER' ORDER BY chapter_sort_key ASC LIMIT 5`, [workId]);
  console.log("Queue:");
  console.table(q.rows);

  pool.end();
}
main().catch(console.error);
