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
  const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_protective_stop'");
  if (res.rows[0]) {
    console.log(res.rows[0].value);
  } else {
    console.log('No protective stop row');
  }
  await pool.end();
}
run();
