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
    SELECT 
      (SELECT COUNT(*) FROM chapters WHERE published_at >= NOW() - INTERVAL '5 minutes') as throughput_5m,
      (SELECT COUNT(*) FROM importer_queue WHERE status = 'IMPORTING') as importing_cnt
  `);
  console.log('Stats:', res.rows[0]);

  await pool.end();
}
run().catch(console.error);
