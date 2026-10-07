import dotenv from 'dotenv';
import pg from 'pg';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;

async function run() {
  const client = new Client({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  await client.connect();

  const works = await client.query(`
    SELECT id, title, slug, cover_id, created_at, updated_at, published, latest_chapter_published_at
    FROM works
    WHERE title ILIKE '%Imperador%' OR slug ILIKE '%imperador%'
    ORDER BY created_at ASC;
  `);

  console.log('=== MATCHING WORKS ===');
  for (const w of works.rows) {
    const chCount = await client.query(`SELECT count(*)::int as count, max(number::numeric) as max_ch FROM chapters WHERE work_id = $1`, [w.id]);
    const mappings = await client.query(`SELECT id, source, source_work_id, source_title FROM importer_work_mappings WHERE work_id = $1`, [w.id]);
    
    // Check if table work_aliases exists
    let aliases = [];
    try {
      const aRes = await client.query(`SELECT alias FROM work_aliases WHERE work_id = $1`, [w.id]);
      aliases = aRes.rows.map(r => r.alias);
    } catch (e) {
      aliases = ['(table work_aliases does not exist)'];
    }

    // Check user data: bookmarks/favorites, reading history, comments
    let favoritesCount = 0, historyCount = 0, commentsCount = 0;
    try {
      const f = await client.query(`SELECT count(*)::int as c FROM user_library WHERE work_id = $1`, [w.id]);
      favoritesCount = f.rows[0].c;
    } catch {}
    try {
      const h = await client.query(`SELECT count(*)::int as c FROM reading_history WHERE work_id = $1`, [w.id]);
      historyCount = h.rows[0].c;
    } catch {}
    try {
      const c = await client.query(`SELECT count(*)::int as c FROM comments WHERE work_id = $1`, [w.id]);
      commentsCount = c.rows[0].c;
    } catch {}

    console.log(`\nID: ${w.id}`);
    console.log(`Title: "${w.title}" | Slug: "${w.slug}"`);
    console.log(`Cover ID: ${w.cover_id}`);
    console.log(`Created: ${w.created_at} | Updated: ${w.updated_at}`);
    console.log(`Published: ${w.published} | Latest Chapter Pub: ${w.latest_chapter_published_at}`);
    console.log(`Chapters count: ${chCount.rows[0].count} | Max chapter: ${chCount.rows[0].max_ch}`);
    console.log(`Mappings (${mappings.rows.length}):`, mappings.rows);
    console.log(`Aliases:`, aliases);
    console.log(`User Data: Favorites=${favoritesCount}, History=${historyCount}, Comments=${commentsCount}`);
  }

  await client.end();
}

run().catch(console.error);
