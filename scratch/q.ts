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

  const qRes = await pool.query(`
    SELECT min(chapter_sort_key) as min_sort
    FROM importer_queue
    WHERE payload->>'workId' = '03d01949-f2e7-4340-89cf-643fae3dcaac'
  `);
  console.log("Min Sort in Queue:", qRes.rows[0].min_sort);

  const cRes = await pool.query(`
    SELECT max(number) as max_pub
    FROM chapters
    WHERE work_id = '03d01949-f2e7-4340-89cf-643fae3dcaac'
  `);
  console.log("Max Pub in Chapters:", cRes.rows[0].max_pub);

  await pool.end();
}

main().catch(console.error).then(() => process.exit(0));
