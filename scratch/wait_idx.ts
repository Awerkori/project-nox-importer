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
  while(true) {
    const res = await pool.query("SELECT indisvalid FROM pg_index JOIN pg_class ON indexrelid = oid WHERE relname = 'idx_importer_mappings_staged'");
    if (res.rows[0]?.indisvalid) break;
    await new Promise(r => setTimeout(r, 2000));
  }
  console.log("Valid");
  await pool.end();
}
run();
