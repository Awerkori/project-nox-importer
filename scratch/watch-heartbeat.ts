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

  console.log("Watching heartbeat...");
  let lastTimestamp = "";
  let lastCapacity = 0;

  for (let i = 0; i < 30; i++) {
    const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
    if (res.rows[0]) {
      const data = JSON.parse(res.rows[0].value);
      if (data.timestamp !== lastTimestamp) {
        lastTimestamp = data.timestamp;
        const capacity = data.capacity?.concurrency || data.capacity;
        console.log(`[${data.timestamp}] Status: ${data.status} | Capacity: ${capacity} | Eligible: ${data.eligibleJobs} | Completed 30m: ${data.completed30m} | Throughput Rate: ${data.throughput?.rate1m || 0}`);
        if (capacity > 1 && data.eligibleJobs < 100) {
          console.log("RECOVERY DETECTED!");
          break;
        }
      }
    }
    await new Promise(r => setTimeout(r, 10000));
  }
  await pool.end();
}
run().catch(console.error);
