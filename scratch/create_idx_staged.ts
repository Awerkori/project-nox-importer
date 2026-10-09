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
  console.log("Creating idx_importer_mappings_staged...");
  await pool.query("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_mappings_staged ON importer_chapter_mappings (work_id ASC, chapter_sort_key ASC) WHERE status IN ('STAGED', 'WAITING_FOR_GAP')");
  console.log("Index created.");
  await pool.end();
}
run();
