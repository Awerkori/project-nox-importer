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
    const res = await pool.query(`
      SELECT indisvalid
      FROM pg_index i
      JOIN pg_class c ON i.indexrelid = c.oid
      WHERE c.relname = 'idx_importer_queue_p1_paused'
    `);
    if (res.rows[0] && res.rows[0].indisvalid === true) {
      console.log("Index is valid!");
      break;
    }
    await new Promise(r => setTimeout(r, 5000));
  }
  await pool.end();
}
run();
