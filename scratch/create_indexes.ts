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

  console.log("Creating idx_importer_queue_p1_claim_source...");
  await pool.query(`
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_queue_p1_claim_source 
    ON importer_queue (source ASC, priority DESC, chapter_sort_key ASC) 
    WHERE status IN ('QUEUED', 'RETRY') AND task_type = 'IMPORT_CHAPTER';
  `);
  
  console.log("Creating idx_importer_queue_p1_paused_source...");
  await pool.query(`
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_queue_p1_paused_source 
    ON importer_queue (source ASC, priority DESC, chapter_sort_key ASC) 
    WHERE status = 'PAUSED_BY_STAFF' AND task_type = 'IMPORT_CHAPTER';
  `);

  console.log("Indexes created.");
  await pool.end();
}
run();
