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
    SELECT state, count(*) 
    FROM pg_stat_activity 
    WHERE query NOT ILIKE '%pg_stat_activity%'
    GROUP BY state;
  `);
  console.log("DB Connections:", res.rows);
  await pool.end();
}
run();
