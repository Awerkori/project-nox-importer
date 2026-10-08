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

  try {
    const res = await pool.query(`
      SELECT COUNT(*) as importing_count
      FROM importer_queue
      WHERE status = 'IMPORTING'
    `);
    console.log("IMPORTING:", res.rows[0].importing_count);
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
run();
