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
      SELECT data->'publicationMetrics'->'sqlExecutionMs'->>'avg' as pub_sql_avg,
             data->'publicationMetrics'->'commitMs'->>'avg' as pub_commit_avg,
             data->'publicationMetrics'->'totalMs'->>'avg' as pub_total_avg,
             data->'latency'->'publishBatchMs'->>'avg' as pub_batch_ms
      FROM importer_diagnostic_telemetry 
      WHERE data->>'publicationMetrics' IS NOT NULL 
         OR data->'latency'->'publishBatchMs' IS NOT NULL
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
