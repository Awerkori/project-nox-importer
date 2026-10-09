import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

async function main() {
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  const res = await pool.query(`
    SELECT data->'throughput'->>'rate1m' as rate1m,
           data->'throughput'->>'status' as status,
           data->'throughput'->>'limitingFactor' as limiting_factor,
           data->'capacity'->>'concurrency' as cap,
           data->'capacity'->>'pressureReason' as pressure
    FROM importer_diagnostic_telemetry
    ORDER BY created_at DESC
    LIMIT 1
  `);
  console.log(res.rows);

  await pool.end();
}

main().catch(console.error).then(() => process.exit(0));
