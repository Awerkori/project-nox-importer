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
  const ids = works.rows.map(w => w.id);
  console.log(`Found ${ids.length} 'entrar na conta' works`);

  const chCount = await client.query('SELECT count(*)::int as cnt FROM chapters WHERE work_id = ANY($1::uuid[])', [ids]);
  const cmCount = await client.query('SELECT count(*)::int as cnt FROM importer_chapter_mappings WHERE work_id = ANY($1::uuid[])', [ids]);
  const wmCount = await client.query('SELECT count(*)::int as cnt FROM importer_work_mappings WHERE work_id = ANY($1::uuid[])', [ids]);
  const qCount = await client.query("SELECT count(*)::int as cnt FROM importer_queue WHERE (payload->>'workId') = ANY($1::text[])", [ids]);

  console.log({
    chapters: chCount.rows[0].cnt,
    chapter_mappings: cmCount.rows[0].cnt,
    work_mappings: wmCount.rows[0].cnt,
    queue_jobs: qCount.rows[0].cnt,
  });

  await client.end();
}
main().catch(err => { console.error(err); process.exit(1); });
