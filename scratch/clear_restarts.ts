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
    await pool.query("UPDATE settings SET value = '[]' WHERE key = 'importer_auto_restarts'");
    console.log("Cleared importer_auto_restarts!");
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
run();
