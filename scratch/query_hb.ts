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
  const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
  console.log(JSON.stringify(JSON.parse(res.rows[0].value), null, 2));
  pool.end();
}
main().catch(console.error);
