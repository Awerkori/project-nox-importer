import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import { AutoHealWatchdog } from './src/core/auto-heal-watchdog.js';
import { diagnostics } from './src/core/diagnostics.js';
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

  const autoHealWatchdog = new AutoHealWatchdog({ pool });
  
  try {
        const now = Date.now();
        const mem = diagnostics.getMemorySnapshot();
        const activeJobsCount = diagnostics.getActiveJobsCount();

        // Query watchdog telemetry first (shared cache, lightweight in HEALTHY)
        const healthMetrics = await autoHealWatchdog.collectTelemetry();
        console.log("healthMetrics collected", healthMetrics.status);
        
  } catch (err: any) {
    console.error("Watchdog cycle error:", err);
  }

  await pool.end();
}
run();
