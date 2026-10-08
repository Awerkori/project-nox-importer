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
      SELECT created_at, CAST(data AS TEXT) as txt
      FROM importer_diagnostic_telemetry
      WHERE CAST(data AS TEXT) ILIKE '%error%'
         OR CAST(data AS TEXT) ILIKE '%crash%'
         OR CAST(data AS TEXT) ILIKE '%fail%'
      ORDER BY created_at DESC
      LIMIT 1
    `);
    if (res.rows.length > 0) {
      console.log(res.rows[0].created_at);
      const data = JSON.parse(res.rows[0].txt);
      console.log(JSON.stringify(data, null, 2).substring(0, 2000));
    } else {
      console.log("No rows found");
    }
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
run();
