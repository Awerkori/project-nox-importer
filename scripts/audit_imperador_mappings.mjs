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
    const workId = '0f5cc9e2-8d56-4fae-af23-4e9301c3d19d';
    const wm = await client.query(`
      SELECT *
      FROM importer_work_mappings
      WHERE work_id = $1::uuid;
    `, [workId]);
    console.log('Work mappings:', wm.rows);

    const ch = await client.query(`
      SELECT *
      FROM chapters
      WHERE work_id = $1::uuid
      ORDER BY number ASC;
    `, [workId]);
    console.log('Chapters in chapters table:', ch.rows);

    const cm = await client.query(`
      SELECT *
      FROM importer_chapter_mappings
      WHERE work_id = $1::uuid
      ORDER BY chapter_sort_key ASC;
    `, [workId]);
    console.log('Chapter mappings:', cm.rows);

    const sources = await client.query(`
      SELECT id, name, enabled, status
      FROM importer_sources;
    `);
    console.log('Sources:', sources.rows);
  } finally {
    await client.end();
  }
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
