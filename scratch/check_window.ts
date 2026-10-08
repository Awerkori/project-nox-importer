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
  const res = await pool.query(`
    SELECT source, COUNT(*) as queued_count 
    FROM importer_queue 
    WHERE status IN ('QUEUED', 'RETRY') AND task_type = 'IMPORT_CHAPTER' 
    GROUP BY source
  `);
  console.log("Queue counts:", res.rows);
  await pool.end();
}
main();
