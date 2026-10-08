import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function run() {
  const ids = [
    'c3017950-fa64-4c31-9eaf-8c6aaf63440b', '6ebe19ca-eda9-4afb-bd64-9e1507bd31d6',
    'b63c2720-61c8-4fe2-8469-c5550d57c645', '2455b9b4-9292-4d6b-b9c1-2307aadde6a7',
    '1185ef16-2b3f-4d86-8475-48cdf9151702', 'e8bf25c8-470b-414a-b606-89f704e25919'
  ];
  
  const q1 = `EXPLAIN ANALYZE SELECT work_id::text, COALESCE(MAX(number), -1) as max_pub
              FROM chapters
              WHERE work_id = ANY($1::uuid[]) AND published_at IS NOT NULL
              GROUP BY work_id`;

  const q2 = `EXPLAIN ANALYZE SELECT work_id::text, start_sort_key, end_sort_key
              FROM importer_confirmed_gaps
              WHERE work_id = ANY($1::uuid[])`;
              
  const res1 = await pool.query(q1, [ids]);
  console.log("=== chapters lookup ===");
  console.log(res1.rows.map(r => r['QUERY PLAN']).join('\n'));

  const res2 = await pool.query(q2, [ids]);
  console.log("=== gaps lookup ===");
  console.log(res2.rows.map(r => r['QUERY PLAN']).join('\n'));

  await pool.end();
}
run().catch(console.error);
