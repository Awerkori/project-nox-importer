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

  console.log("Waiting for new deploy to take effect...");
  let lastSha = '';
  for (let i = 0; i < 300; i++) { // ~50 minutes
    try {
      const res = await pool.query(`
        SELECT data->'runtimeFingerprint'->>'gitSha' as sha,
               data->'schedulerAcquireBreakdown'->'claimLockSqlExecMs'->>'avg' as claim_sql_avg,
               data->'schedulerAcquireBreakdown'->'totalMs'->>'avg' as total_acq_avg,
               data->'yugabyteDbPool'->>'waitP50Ms' as pool_wait_p50,
               data->'yugabyteDbPool'->>'waitAvgMs' as pool_wait_avg
        FROM importer_diagnostic_telemetry 
        WHERE data->>'schedulerAcquireBreakdown' IS NOT NULL
           OR data->>'yugabyteDbPool' IS NOT NULL
        ORDER BY created_at DESC 
        LIMIT 1
      `);
      const hbRes = await pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
      let hb = null;
      if (hbRes.rows[0]) hb = JSON.parse(hbRes.rows[0].value);

      if (res.rows[0]) {
        const row = res.rows[0];
        console.log(`[${new Date().toISOString()}] SHA: ${row.sha?.substring(0,7)} | ClaimAvg: ${row.claim_sql_avg}ms | PoolWaitP50: ${row.pool_wait_p50}ms | PoolWaitAvg: ${row.pool_wait_avg}ms | Status: ${hb?.status} | rate1m: ${hb?.throughput?.rate1m ?? hb?.rate1m} | Cap: ${hb?.capacity?.concurrency ?? hb?.capacity} | 5xx: ${hb?.capacity?.consecutive5xx}`);
      }
    } catch (err) {
      console.error(err.message);
    }
    await new Promise(resolve => setTimeout(resolve, 10000));
  }
  await pool.end();
}
run();
