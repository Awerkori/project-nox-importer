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

  const res = await pool.query(`EXPLAIN ANALYZE SELECT count(*) FROM importer_chapter_mappings WHERE status = 'WAITING_FOR_GAP'`);
  console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));

  await pool.end();
}
run();
