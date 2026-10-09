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

  while (true) {
    const res = await pool.query(`SELECT indexname, indisready, indisvalid FROM pg_indexes i JOIN pg_class c ON i.indexname = c.relname JOIN pg_index x ON c.oid = x.indexrelid WHERE indexname LIKE 'idx_importer_queue_p1_%_source'`);
    console.log(res.rows);
    if (res.rows.length >= 2 && res.rows.every(r => r.indisvalid)) break;
    await new Promise(r => setTimeout(r, 5000));
  }
  console.log("Indexes are ready!");
  await pool.end();
}
run();
