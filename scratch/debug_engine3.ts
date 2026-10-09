import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import { ImporterEngine } from '../src/core/engine.js';
import { telemetryCollector } from '../src/core/telemetry-collector.js';
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
     console.log("Checking limits...");
     const slotRuntime = telemetryCollector.getSlotProductivitySnapshot();
     const chapterLimiter = engine.autotuner.getGlobalChapterSemaphore();
     const mediaLimiter = engine.autotuner.getGlobalMediaSemaphore();
     const downloadLimiter = engine.autotuner.getGlobalInflightRequestSemaphore();
     const bufferLimiter = engine.autotuner.getBufferedPageSemaphore();
     
     console.log("Limits OK");
  } catch (err: any) {
    console.error("Watchdog cycle error:", err);
  }

  await pool.end();
}
run();
