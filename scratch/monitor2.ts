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

  for(let i=0; i<15; i++) {
    const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
    if (res.rows[0]) {
      const h = JSON.parse(res.rows[0].value);
      console.log(`[${new Date().toISOString()}] pid: ${h.pid} | eligibleJobs: ${h.eligibleJobs} | capacity: ${h.capacity?.concurrency} | state: ${h.state} | ts: ${h.timestamp}`);
    }
    await new Promise(r => setTimeout(r, 10000));
  }
  await pool.end();
}
run().catch(console.error);
