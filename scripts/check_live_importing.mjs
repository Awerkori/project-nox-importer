import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 10000,
  query_timeout: 15000,
});

async function run() {
  await client.connect();
  try {
    const res = await client.query(`
      SELECT q.id, q.source, q.task_type, q.priority, (q.payload->>'staffForced') as staff_forced,
             (q.payload->>'workId') as work_id, w.title as work_title, q.chapter_sort_key,
             q.status, q.locked_by, q.locked_at, q.updated_at
      FROM importer_queue q
      LEFT JOIN works w ON (q.payload->>'workId')::uuid = w.id
      WHERE q.status = 'IMPORTING' OR q.updated_at >= NOW() - INTERVAL '2 minutes'
      ORDER BY q.updated_at DESC
      LIMIT 15;
    `);
    console.log('--- RECENTLY ACTIVE OR IMPORTING JOBS ---');
    console.log(JSON.stringify(res.rows, null, 2));

    const counts = await client.query(`
      SELECT status, count(*) as cnt
      FROM importer_queue
      WHERE (payload->>'workId') = '623c6884-749c-4ebf-94ea-3fe9f9fc501c'
      GROUP BY status;
    `);
    console.log('--- MAGO INFINITO JOB STATUS BREAKDOWN ---');
    console.log(JSON.stringify(counts.rows, null, 2));
  } finally {
    await client.end();
  }
}

run().catch(console.error);
