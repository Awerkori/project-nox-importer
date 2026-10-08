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
    const res = await pool.query(`
      SELECT created_at, 
             data->'schedulerAcquireBreakdown'->'claimLockSqlExecMs'->>'avg' as claim_sql_avg,
             data->'schedulerAcquireBreakdown'->'claimLockSqlExecMs'->>'p95' as claim_sql_p95,
             data->'schedulerAcquireBreakdown'->'totalMs'->>'avg' as total_acq_avg,
             data->'runtimeFingerprint'->>'gitSha' as sha
      FROM importer_diagnostic_telemetry 
      WHERE data->>'schedulerAcquireBreakdown' IS NOT NULL
      ORDER BY created_at DESC 
      LIMIT 10
    `);
    console.table(res.rows);
  } catch (err) {
    console.error(err.message);
  }
  await pool.end();
}
run();
