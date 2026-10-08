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
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_queue_p1_paused 
    ON public.importer_queue USING lsm (priority DESC, chapter_sort_key ASC) 
    WHERE ((status = 'PAUSED_BY_STAFF'::text) AND (task_type = 'IMPORT_CHAPTER'::text));
  `;

  try {
    const res = await pool.query(query);
    console.log("Index created:", res);
  } catch(e) {
    console.error(e);
  }
  await pool.end();
}
run();
