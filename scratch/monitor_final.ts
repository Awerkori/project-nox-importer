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

  console.log("Checking production heartbeat for throughput...");
  for (let i = 0; i < 100; i++) { // ~10 minutes
    try {
      const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
      if (res.rows[0]) {
        const data = JSON.parse(res.rows[0].value);
        console.log(`[${data.timestamp}] Status: ${data.status} | Cap: ${data.capacity?.concurrency ?? data.capacity} | Busy Slots: ${data.pipelineCapacity?.slots?.busySlots} | Eligible: ${data.eligibleJobs} | rate1m: ${data.throughput?.rate1m ?? data.rate1m} | Completed30m: ${data.completed30m}`);
      }
    } catch (err) {
      console.error(err.message);
    }
    await new Promise(resolve => setTimeout(resolve, 6000));
  }
  await pool.end();
}
run();
