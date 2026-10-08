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

  const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
  if (res.rows[0]) {
    const data = JSON.parse(res.rows[0].value);
    console.log("Status:", data.status);
    console.log("Capacity:", data.capacity.concurrency);
    console.log("Eligible Jobs:", data.eligibleJobs);
    console.log("Stuck Staged:", data.stuckStaged);
    console.log("Active Works:", data.activeWorksCount || data.pipelineCapacity?.slots?.effectiveSlots);
    console.log("Completed 30m:", data.completed30m);
    console.log("Timestamp:", data.timestamp);
  }
  await pool.end();
}
run().catch(console.error);
