import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
});

async function main() {
  const { SOURCE_EXECUTION_ELIGIBILITY_SQL } = await import('../build/core/source-eligibility.js');
  const { CANONICAL_PUBLISHED_CLAIM_FILTER, CANONICAL_FRONTIER_CLAIM_FILTER, CANONICAL_ACTIVE_CLAIM_FILTER } = await import('../build/core/scheduler/work-affinity-scheduler.js');
  
  console.log("CANONICAL_PUBLISHED_CLAIM_FILTER:\n", CANONICAL_PUBLISHED_CLAIM_FILTER);
  console.log("CANONICAL_FRONTIER_CLAIM_FILTER:\n", CANONICAL_FRONTIER_CLAIM_FILTER);
  console.log("CANONICAL_ACTIVE_CLAIM_FILTER:\n", CANONICAL_ACTIVE_CLAIM_FILTER);

  pool.end();
}
main().catch(console.error);
