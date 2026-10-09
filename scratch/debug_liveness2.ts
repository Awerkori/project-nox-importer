import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import { AutoHealWatchdog } from '../src/core/auto-heal-watchdog.js';
import { diagnostics } from '../src/core/diagnostics.js';
import { ImporterEngine } from '../src/core/engine.js';
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

  const engine = new ImporterEngine({ pool, dbHost: 'test', WORKER_ID: 'debug' } as any);
  
  try {
     console.log("Running autoHealWatchdog.collectTelemetry()...");
     const healthMetrics = await engine.autoHealWatchdog.collectTelemetry();
     console.log("healthMetrics collected", healthMetrics.status);
     
     // Let's also check throughput data since that is next in runLivenessWatchdogLoop
     console.log("Running throughput calculations...");
     const tp = engine.rateBucketTracker.getThroughputSnapshot(1, 0, 0, false);
     console.log("throughputData", tp);
     
  } catch (err: any) {
    console.error("Watchdog cycle error:", err);
  }

  await pool.end();
}
run();
