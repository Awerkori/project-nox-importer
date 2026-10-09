import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import { confirmUpstreamGapInterval } from '../src/core/gap-validator.js';

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
  const res = await confirmUpstreamGapInterval(pool, {
    workId: '1da06bc3-ba83-44b8-907d-dc5923816caf',
    startSortKey: 29,
    endSortKey: 29,
    primarySource: 'apenasumafa',
    reason: 'TEST',
  });
  console.log(JSON.stringify(res, null, 2));
  pool.end();
}
main().catch(console.error);
