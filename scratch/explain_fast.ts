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
  const res = await pool.query(`EXPLAIN ANALYZE SELECT work_id FROM importer_chapter_mappings WHERE status = 'STAGED' AND work_id IS NOT NULL GROUP BY work_id ORDER BY MIN(chapter_sort_key) ASC LIMIT 100`);
  console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  await pool.end();
}
run();
