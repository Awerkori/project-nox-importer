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

  console.log(`Analyzing ${dupes.rows.length} duplicate groups`);

  let bothHaveChaptersCount = 0;
  let oneHasChaptersCount = 0;
  let zeroChaptersCount = 0;

  for (const r of dupes.rows) {
    const list = [];
    for (const wid of r.ids) {
      const wRes = await client.query('SELECT id, slug, published FROM works WHERE id = $1::uuid', [wid]);
      const chRes = await client.query('SELECT count(*)::int as count FROM chapters WHERE work_id = $1::uuid', [wid]);
      list.push({
        id: wid,
        slug: wRes.rows[0].slug,
        published: wRes.rows[0].published,
        chapters: chRes.rows[0].count
      });
    }

    const withChapters = list.filter(x => x.chapters > 0);
    if (withChapters.length > 1) {
      bothHaveChaptersCount++;
      console.log(`[BOTH HAVE CHAPTERS] "${r.norm_title}":`, list);
    }
  }

  console.log(`\nSummary:
  Both have chapters: ${bothHaveChaptersCount}
  One has chapters: ${oneHasChaptersCount}
  Both have 0 chapters: ${zeroChaptersCount}
  `);

  await client.end();
}
main().catch(err => { console.error(err); process.exit(1); });
