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
    SELECT q.*, s.status as source_status, s.enabled
    FROM importer_queue q
    JOIN importer_sources s ON s.id = q.source
    WHERE q.id = '29a141db-e778-42c2-9aa0-d981656f96a5'
  `);
  console.log('Job details:', res.rows[0]);

  const pub = await pool.query(`
    SELECT MAX(c.number) AS max_published
    FROM chapters c
    WHERE c.work_id = 'b63c2720-61c8-4fe2-8469-c5550d57c645'
      AND c.published_at IS NOT NULL
  `);
  console.log('Max published:', pub.rows[0]);

  await pool.end();
}
run().catch(console.error);
