import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

async function run() {
  const client = new pg.Client({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });
  await client.connect();
  await client.query('SET statement_timeout = 15000;');

  const works = await client.query(`
    SELECT id, title, slug, aliases, author, artist, kind, status, published, cover_id, created_at, updated_at
    FROM works
    WHERE title ILIKE '%Imperador%' OR title ILIKE '%Servant%' OR slug ILIKE '%imperador%' OR slug ILIKE '%servant%'
  `);
  console.log('WORKS FOUND:', JSON.stringify(works.rows, null, 2));

  for (const w of works.rows) {
    const mappings = await client.query(`
      SELECT id, source, source_work_id, source_slug, source_title, sync_status, is_primary
      FROM importer_work_mappings
      WHERE work_id = $1
    `, [w.id]);
    console.log(`MAPPINGS FOR ${w.id} (${w.title}):`, JSON.stringify(mappings.rows, null, 2));

    const chCount = await client.query(`
      SELECT count(*) as count, min(number) as min_ch, max(number) as max_ch
      FROM chapters
      WHERE work_id = $1
    `, [w.id]);
    console.log(`CHAPTERS FOR ${w.id}:`, chCount.rows[0]);

    const queueCount = await client.query(`
      SELECT status, count(*)
      FROM importer_queue
      WHERE (payload->>'workId') = $1
      GROUP BY status
    `, [w.id]);
    console.log(`QUEUE FOR ${w.id}:`, queueCount.rows);
  }

  await client.end();
}
run().catch(console.error);
