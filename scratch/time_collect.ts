import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import { AutoHealWatchdog } from '../src/core/auto-heal-watchdog.js';
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

  const watchdog = new AutoHealWatchdog({
    pool,
  });

  console.log("Starting telemetry collection...");
  const start = Date.now();
  const tel = await watchdog.collectTelemetry(true);
  console.log("Telemetry collected in", Date.now() - start, "ms");

  await pool.end();
}
run();
