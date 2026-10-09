import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import { TelemetryCollector } from '../src/core/telemetry-collector.js';
import { RateBucketTracker } from '../src/core/rate-bucket-tracker.js';
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

  const tracker = new RateBucketTracker(pool, null as any);

  console.log("Running getRecentRates()...");
  const start = Date.now();
  const rates = await tracker.getRecentRates();
  console.log("Done in", Date.now() - start, "ms", rates);

  await pool.end();
}
run();
