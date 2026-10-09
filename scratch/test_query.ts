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

  console.log("Running stale IMPORTING query...");
  const start = Date.now();
  const staleRes = await pool.query(
          `SELECT COUNT(*) as stale_importing_cnt
           FROM importer_queue 
           WHERE status = 'IMPORTING'
             AND (
               lease_expires_at <= NOW()
               OR (lease_expires_at IS NULL AND (locked_at <= NOW() - INTERVAL '5 minutes' OR updated_at <= NOW() - INTERVAL '5 minutes'))
               OR (locked_at <= NOW() - INTERVAL '15 minutes' AND updated_at <= NOW() - INTERVAL '15 minutes')
             )`
        );
  console.log("Done in", Date.now() - start, "ms", staleRes.rows);

  await pool.end();
}
run();
