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

  try {
    await pool.query("DROP INDEX IF EXISTS idx_importer_queue_p1_paused;");
    console.log("Dropped invalid index");
    await pool.query(`
      CREATE INDEX idx_importer_queue_p1_paused 
      ON public.importer_queue USING lsm (priority DESC, chapter_sort_key ASC) 
      WHERE ((status = 'PAUSED_BY_STAFF'::text) AND (task_type = 'IMPORT_CHAPTER'::text));
    `);
    console.log("Created index successfully");
  } catch(e) {
    console.error(e);
  }
  await pool.end();
}
run();
