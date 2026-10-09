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
  const res = await pool.query(`
    SELECT status, COUNT(*) as count 
    FROM importer_queue 
    WHERE task_type = 'IMPORT_CHAPTER' 
    GROUP BY status
  `);
  console.log('importer_queue:', res.rows);
  const mappings = await pool.query(`
    SELECT status, COUNT(*) as count 
    FROM importer_chapter_mappings 
    GROUP BY status
  `);
  console.log('importer_chapter_mappings:', mappings.rows);
  await pool.end();
}
run();
