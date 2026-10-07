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

  const dupes = await client.query(`
    SELECT lower(trim(title)) as norm_title, array_agg(id::text) as ids
    FROM works
    WHERE lower(trim(title)) != 'entrar na conta'
    GROUP BY lower(trim(title))
    HAVING count(*) > 1
    ORDER BY lower(trim(title))
  `);

  console.log(`Found ${dupes.rows.length} duplicate groups`);

  for (const r of dupes.rows) {
    console.log('\n======================================================');
    console.log('TITLE:', r.norm_title);
    for (const wid of r.ids) {
      const wRes = await client.query('SELECT id, slug, published, created_at FROM works WHERE id = $1::uuid', [wid]);
      const w = wRes.rows[0];
      const chRes = await client.query('SELECT count(*)::int as count FROM chapters WHERE work_id = $1::uuid', [wid]);
      const chCount = chRes.rows[0].count;
      const mapRes = await client.query('SELECT source, source_work_id FROM importer_work_mappings WHERE work_id = $1::uuid', [wid]);
      console.log(`  Work: ${w.id} | Slug: ${w.slug} | Published: ${w.published} | Chapters: ${chCount} | Mappings: ${mapRes.rows.map(m => m.source + ':' + m.source_work_id).join(', ')}`);
    }
  }

  await client.end();
}
main().catch(err => { console.error(err); process.exit(1); });
