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
  const res = await pool.query("SELECT id, status, locked_at, extract(epoch from (now() - locked_at)) as age_sec FROM importer_queue WHERE status = 'IMPORTING' AND task_type = 'IMPORT_CHAPTER' ORDER BY locked_at ASC;");
  console.log(res.rows);
  await pool.end();
}
run().catch(console.error);
