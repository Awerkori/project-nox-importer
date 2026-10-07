import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;
const client = new Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  await client.connect();

  const works = await client.query(`
    SELECT id, slug FROM works WHERE lower(trim(title)) = 'entrar na conta'
  `);
  console.log('Total entrar na conta works:', works.rows.length);

  for (const w of works.rows) {
    const map = await client.query('SELECT id, source, source_work_id, source_slug, source_title FROM importer_work_mappings WHERE work_id = $1::uuid', [w.id]);
    const q = await client.query(`SELECT count(*)::int as count FROM importer_queue WHERE (payload->>'workId') = $1`, [w.id]);
    console.log(`Work: ${w.id} | slug: ${w.slug} | mappings: ${map.rows.length} | queue jobs: ${q.rows[0].count}`);
    for (const m of map.rows) {
      console.log(`   source: ${m.source} | src_title: ${m.source_title} | src_slug: ${m.source_slug}`);
    }
  }

  await client.end();
}
main().catch(err => { console.error(err); process.exit(1); });
