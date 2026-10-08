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

  console.log("Starting production monitor...");
  let lastTimestamp = "";
  
  // Track throughput manually
  const publishedCounts: number[] = [];
  
  for (let i = 0; i < 40; i++) { // ~4 minutes
    try {
      const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
      if (res.rows[0]) {
        const data = JSON.parse(res.rows[0].value);
        if (data.timestamp !== lastTimestamp) {
          lastTimestamp = data.timestamp;
          const cap = data.capacity?.concurrency ?? data.capacity ?? 0;
          const slots = data.pipelineCapacity?.slots?.busySlots ?? 0;
          const rate1m = data.throughput?.rate1m ?? data.rate1m ?? 0;
          const completed30m = data.completed30m ?? 0;
          const eligible = data.eligibleJobs ?? 0;
          const status = data.status;
          
          console.log(`[${data.timestamp}] Status: ${status} | Cap: ${cap} | Busy Slots: ${slots} | Eligible: ${eligible} | Completed 30m: ${completed30m} | Rate1m: ${rate1m}`);
        }
      }
    } catch (err) {
      console.error("Error fetching heartbeat:", err.message);
    }
    await new Promise(resolve => setTimeout(resolve, 6000));
  }
  await pool.end();
}
run().catch(console.error);
