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
    SELECT status, count(*) 
    FROM importer_chapter_mappings 
    WHERE work_id = '0003c726-f6c6-4e90-ad0a-2a04a0e4d771'
    GROUP BY status
  `);
  console.log(res.rows);

  await pool.end();
}
run();
