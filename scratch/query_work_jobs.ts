import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

async function main() {
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  const res = await pool.query(`
    SELECT id, chapter_sort_key, status, next_run_at
    FROM importer_queue
    WHERE payload->>'workId' = '1da06bc3-8515-46e7-ac6c-f2305886af3f'
      AND task_type = 'IMPORT_CHAPTER'
    ORDER BY chapter_sort_key ASC
    LIMIT 10
  `);
  console.table(res.rows);

  await pool.end();
}

main().catch(console.error).then(() => process.exit(0));
