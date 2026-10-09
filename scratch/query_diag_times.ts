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
      SELECT 
        data->'jobProfile' as job_profile,
        data->'telegramStorage' as tg_storage,
        data->'imageDownload' as download
      FROM importer_diagnostic_telemetry 
      ORDER BY created_at DESC LIMIT 1
    `);
    console.log(JSON.stringify(res.rows[0], null, 2));
  } catch (err) {
    console.error(err.message);
  }
  await pool.end();
}
run();
