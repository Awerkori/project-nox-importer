import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
});

async function main() {
  const res = await pool.query(`
    SELECT id, source, status, priority, chapter_sort_key 
    FROM importer_queue 
    WHERE id IN ('419b557c-482c-4387-a4a6-decd86870a1a', '7646039c-4990-41ba-a6d2-6dda24b2f24f');
  `);
  console.table(res.rows);
  
  // also check active jobs per source
  const act = await pool.query(`
    SELECT source, COUNT(*) as c
    FROM importer_queue
    WHERE status IN ('IMPORTING', 'ACTIVE', 'RETRYING')
    GROUP BY source
  `);
  console.log("Active by source:");
  console.table(act.rows);

  pool.end();
}
main().catch(console.error);
