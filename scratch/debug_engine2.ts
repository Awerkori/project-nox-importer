import { Pool } from 'pg';
import * as dotenv from 'dotenv';
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

  const engine = new ImporterEngine(null as any, null as any, null as any, null as any, { WORKER_ID: 'debug', INSTANCE_ID: 'debug' } as any, pool);
  
  try {
     console.log("Running runLivenessWatchdogLoop tick...");
     const healthMetrics = await engine.autoHealWatchdog.collectTelemetry();
     console.log("Health OK", healthMetrics.status);
     const isStopActive = await engine.protectiveSentinel.isProtectiveStopActive();
     const rateMetrics = await engine.rateBucketTracker.getRecentRates();
     console.log("Rate OK", rateMetrics);

     const candidateSources = engine.activeSourcesCache.sources.length > 0 ? engine.activeSourcesCache.sources : ['test'];
     const allSourcesBlocked = !candidateSources.some((src: string) => engine.circuitBreaker.canExecute(src));

     const throughputContext = {
       eligibleJobs: healthMetrics.eligibleJobs,
       stagedDebt: healthMetrics.publishableStaged,
       allSourcesBlocked,
       canonicalRate5m: rateMetrics.rate5m,
     };
     const throughputData = engine.autotuner.getThroughputTelemetry(throughputContext);
     console.log("Throughput OK", throughputData.status);
     
  } catch (err: any) {
    console.error("Watchdog cycle error:", err);
  }

  await pool.end();
}
run();
