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

  const t0 = Date.now();
  console.log("Running staged unique query...");
  try {
    const mapRes = await pool.query(`
      SELECT count(*) as staged_unique
      FROM (
        SELECT DISTINCT work_id, chapter_sort_key
        FROM importer_chapter_mappings
        WHERE status IN ('STAGED', 'WAITING_FOR_GAP')
      ) sub
    `);
    console.log("Success:", mapRes.rows, "Time:", Date.now() - t0);
  } catch (err) {
    console.error("Error:", err.message);
  }
  await pool.end();
}
run();
