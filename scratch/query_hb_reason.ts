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
    const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
    if (res.rows.length > 0) {
      const hb = JSON.parse(res.rows[0].value);
      console.log("Capacity:", JSON.stringify(hb.capacity, null, 2));
      console.log("Status:", hb.status, "Reason:", hb.noProgressReason);
      console.log("Last AutoHeal:", hb.lastAutoHeal);
    }
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
run();
