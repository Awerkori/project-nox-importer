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

  const q = await pool.query(`SELECT id, last_error FROM importer_queue WHERE payload->>'workId' = $1 AND task_type = 'IMPORT_CHAPTER' AND chapter_sort_key = 29 ORDER BY priority DESC`, [workId]);
  console.log("Errors:");
  console.table(q.rows);

  pool.end();
}
main().catch(console.error);
