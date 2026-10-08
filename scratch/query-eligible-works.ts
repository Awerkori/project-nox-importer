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

  const res = await pool.query(`
    SELECT w.id, w.published 
    FROM works w 
    WHERE w.id IN (
      '2455b9b4-9292-4d6b-b9c1-2307aadde6a7',
      '2a2f4af3-7c2f-483f-9801-d88f76b3706b',
      '0f43aad4-459f-4fb5-be3a-a5e034e36487',
      '43c4c54c-9b24-4f9a-96dd-8c2acf5073ea'
    )
  `);
  console.log(res.rows);
  await pool.end();
}
run();
