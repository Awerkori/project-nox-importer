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
    SELECT *
    FROM importer_confirmed_gaps
    WHERE work_id = '03d01949-f2e7-4340-89cf-643fae3dcaac'
  `);
  console.log(res.rows);

  await pool.end();
}

main().catch(console.error).then(() => process.exit(0));
