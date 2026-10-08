import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

pool.query("SELECT payload->>'rate1m' as rate1m, payload->>'capacity' as cap, payload->>'activeSlots' as busy, payload->>'eligibleJobs' as eligible, payload->>'stallType' as stallType, created_at FROM system_telemetry WHERE type = 'engine_heartbeat' ORDER BY created_at DESC LIMIT 10")
  .then(res => { console.table(res.rows); pool.end(); })
  .catch(err => { console.error(err); pool.end(); });
