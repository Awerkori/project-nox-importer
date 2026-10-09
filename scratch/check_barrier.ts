import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import { PublicationSafetyBarrier } from '../src/core/publication-safety-barrier.js';
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

  const barrier = new PublicationSafetyBarrier(pool, { info: console.log, warn: console.log, error: console.error } as any);
  
  const works = [
    '0003c726-f6c6-4e90-ad0a-2a04a0e4d771',
    '00062cce-dd03-4cbe-9e31-91adc4f3004a',
    '0007b29d-f104-4423-8001-3af0852518fa'
  ];
  
  for (const w of works) {
      console.log("Checking work:", w);
      const res = await barrier.evaluatePublicationSafety(w, 1.0000);
      console.log(res);
  }

  await pool.end();
}
run();
