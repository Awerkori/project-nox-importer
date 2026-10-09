import { Pool } from 'pg';
import { confirmUpstreamGapInterval } from '../src/core/gap-validator.js';
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

  const res = await confirmUpstreamGapInterval(pool as any, {
    workId: '03d01949-f2e7-4340-89cf-643fae3dcaac',
    startSortKey: 2,
    endSortKey: 7,
    primarySource: 'some_source',
    reason: 'TEST'
  });
  console.log(JSON.stringify(res, null, 2));

  await pool.end();
}

main().catch(console.error).then(() => process.exit(0));
