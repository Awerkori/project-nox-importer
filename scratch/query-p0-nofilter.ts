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
  const res = await pool.query("SELECT count(*) FROM importer_queue WHERE priority >= 100 AND status IN ('QUEUED', 'RETRY') AND task_type = 'IMPORT_CHAPTER';");
  console.log("P0 Count:", res.rows[0]);
  await pool.end();
}
run().catch(console.error);
