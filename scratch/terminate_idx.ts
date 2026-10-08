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

  const query = `
    SELECT pg_terminate_backend(pid), query
    FROM pg_stat_activity 
    WHERE state = 'active' AND pid <> pg_backend_pid() AND query LIKE '%CREATE INDEX CONCURRENTLY%';
  `;

  try {
    const res = await pool.query(query);
    console.log("Terminated index queries:", res.rows);
  } catch(e) {
    console.error(e);
  }
  await pool.end();
}
run();
