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

  const res = await pool.query(`
    SELECT created_at, 
           data->>'healthMetrics' as health,
           data->>'yugabyteDbPool' as pool
    FROM importer_diagnostic_telemetry
    ORDER BY created_at DESC 
    LIMIT 2
  `);
  
  if (res.rows.length > 0) {
    console.log("Health:", res.rows[0].health);
    console.log("Pool:", res.rows[0].pool);
  }

  await pool.end();
}
run();
