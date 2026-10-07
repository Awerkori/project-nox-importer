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
    console.log('--- 1. STAFF REQUESTS ---');
    const sr = await client.query(`
      SELECT *
      FROM importer_staff_requests
      ORDER BY priority_boost DESC NULLS LAST, created_at ASC;
    `);
    console.log(JSON.stringify(sr.rows, null, 2));

    console.log('--- 2. WORKS INFO ---');
    const works = await client.query(`
      SELECT id, title, slug, published
      FROM works
      WHERE title ILIKE '%Mago Infinito%' OR title ILIKE '%imperador está grávido%'
         OR id IN ('623c6884-749c-4ebf-94ea-3fe9f9fc501c', '0f5cc9e2-8d56-4fae-af23-4e9301c3d19d');
    `);
    console.log(JSON.stringify(works.rows, null, 2));

    for (const w of works.rows) {
      console.log(`\n=== QUEUE STATS FOR WORK ${w.title} (${w.id}) ===`);
      const qStats = await client.query(`
        SELECT status, task_type, priority, (payload->>'staffForced') as staff_forced, count(*) as cnt,
               min(next_run_at) as min_next_run, max(next_run_at) as max_next_run
        FROM importer_queue
        WHERE payload->>'workId' = $1
        GROUP BY status, task_type, priority, (payload->>'staffForced');
      `, [w.id]);
      console.log('Queue rows:', JSON.stringify(qStats.rows, null, 2));

      const chCount = await client.query(`
        SELECT count(*) as total_chapters, max(number) as max_num, min(number) as min_num
        FROM chapters
        WHERE work_id = $1::uuid;
      `, [w.id]);
      console.log(`Chapters in DB:`, chCount.rows[0]);

      const mapCount = await client.query(`
        SELECT status, count(*) as cnt
        FROM importer_chapter_mappings
        WHERE work_id = $1::uuid
        GROUP BY status;
      `, [w.id]);
      console.log(`Chapter mappings in DB:`, mapCount.rows);
    }
  } finally {
    await client.end();
  }
}

run().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
