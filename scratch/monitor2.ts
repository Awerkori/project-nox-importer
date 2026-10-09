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

  for (let i = 0; i < 30; i++) {
    const hbRes = await pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
    if (hbRes.rows[0]) {
      const hb = JSON.parse(hbRes.rows[0].value);
      console.log(`[${new Date().toISOString()}] rate1m: ${hb.throughput?.rate1m} | Cap: ${hb.capacity?.concurrency} | LimitingFactor: ${hb.throughput?.limitingFactor} | ActiveWorks: ${hb.activeWorks}`);
    }
    await new Promise(resolve => setTimeout(resolve, 10000));
  }
  await pool.end();
}
run();
