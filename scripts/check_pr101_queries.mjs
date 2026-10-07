import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: {
    rejectUnauthorized: true,
    ca: fs.readFileSync('./config/root.crt').toString()
  }
});

async function main() {
  const client = await pool.connect();
  try {
    const res = await client.query(`
      SELECT 
        calls,
        round(total_exec_time::numeric, 2) as total_ms,
        round(mean_exec_time::numeric, 2) as mean_ms,
        round(max_exec_time::numeric, 2) as max_ms,
        rows,
        left(query, 240) as query_prefix
      FROM pg_stat_statements
      WHERE query ILIKE '%staged_works%' 
         OR query ILIKE '%recent_completed_jobs%'
         OR query ILIKE '%correlated%'
         OR query ILIKE '%publishable_works%'
         OR query ILIKE '%importer_heartbeat%'
      ORDER BY total_exec_time DESC;
    `);
    console.log("=== PR #101 SPECIFIC QUERIES IN PG_STAT_STATEMENTS ===");
    console.log(JSON.stringify(res.rows, null, 2));
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
