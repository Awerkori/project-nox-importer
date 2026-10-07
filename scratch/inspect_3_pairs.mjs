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

  for (const wid of [
    '18f8ffd8-5d62-4df8-8d72-5763776a5f35', '0b519f76-3a1d-4835-b487-c73bd91c656f',
    'a334d203-eb0c-4750-9828-a6437883d8d0', '0d291b0f-103b-4f46-a4b5-516ee529a8a6',
    'c7421afb-3f9b-4427-840f-4c75f72c95e6', '5e18a83e-26a3-4283-b3ed-f54dab5f15f8'
  ]) {
    const w = (await client.query('SELECT id, title, slug FROM works WHERE id = $1::uuid', [wid])).rows[0];
    const ch = (await client.query('SELECT id, number, title, published_at FROM chapters WHERE work_id = $1::uuid ORDER BY number ASC', [wid])).rows;
    console.log(`Work ${w.id} (${w.slug}): ${ch.length} chapters`);
    for (const c of ch) {
      const p = (await client.query('SELECT count(*)::int as cnt FROM pages WHERE chapter_id = $1::uuid', [c.id])).rows[0];
      console.log(`   ch ${c.number} (${c.title}): ${p.cnt} pages, published: ${c.published_at}`);
    }
  }

  await client.end();
}
main().catch(err => { console.error(err); process.exit(1); });
