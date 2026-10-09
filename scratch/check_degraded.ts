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

  const hbRes = await pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
  if (hbRes.rows[0]) console.log(JSON.stringify(JSON.parse(hbRes.rows[0].value), null, 2));

  await pool.end();
}
run();
