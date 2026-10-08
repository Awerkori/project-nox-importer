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

  const res = await pool.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
  if (res.rows[0]) {
    const activeWorks = res.rows[0].value;
    console.log(JSON.stringify(activeWorks, null, 2));
  }

  await pool.end();
}
run().catch(console.error);
