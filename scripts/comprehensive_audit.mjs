import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function run() {
  const client = await pool.connect();
  try {
    console.log('=== 1. CONVERGÊNCIA P1 E OBRAS ===');
    // Works stats
    const workStats = await client.query(`
      SELECT 
        count(*) as total_works,
        count(*) FILTER (WHERE sync_status = 'CAUGHT_UP') as caught_up,
        count(*) FILTER (WHERE sync_status = 'ACTIVE') as active_incomplete,
        count(*) FILTER (WHERE sync_status = 'WAITING_ADMISSION') as waiting_p3,
        count(*) FILTER (WHERE created_at > NOW() - INTERVAL '3 hours') as new_works_3h
      FROM works
    `);
    console.log('WORK STATS:', workStats.rows[0]);

    // Check duplicate works by slug or title
    console.log('\n=== 2. CANONICALIZAÇÃO & DUPLICATAS ===');
    const dupSlugs = await client.query(`
      SELECT slug, count(*) as cnt FROM works GROUP BY slug HAVING count(*) > 1
    `);
    console.log('DUPLICATE WORK SLUGS:', dupSlugs.rows);

    const dupTitles = await client.query(`
      SELECT lower(title) as norm_title, count(*) as cnt FROM works GROUP BY lower(title) HAVING count(*) > 1
    `);
    console.log('DUPLICATE WORK TITLES:', dupTitles.rows);

    // Duplicate chapters
    const dupChapters = await client.query(`
      SELECT work_id, number, count(*) as cnt 
      FROM chapters 
      WHERE created_at > NOW() - INTERVAL '3 hours'
      GROUP BY work_id, number 
      HAVING count(*) > 1
    `);
    console.log('DUPLICATE CHAPTERS (3H):', dupChapters.rowCount);

    // Canonical missing available chapters
    console.log('\n=== 3. CAPÍTULOS FALTANTES CANÔNICOS ===');
    const missingRes = await client.query(`
      SELECT count(DISTINCT (m.work_id, m.chapter_sort_key)) as missing_canonical
      FROM importer_chapter_mappings m
      JOIN works w ON w.id = m.work_id
      WHERE m.status IN ('PENDING', 'QUEUED', 'IMPORTING')
        AND w.sync_status = 'ACTIVE'
    `);
    console.log('CANONICAL_MISSING_AVAILABLE_P1:', missingRes.rows[0].missing_canonical);

    // All sources mappings
    const totalMappings = await client.query(`
      SELECT count(*) as total_mappings
      FROM importer_chapter_mappings
      WHERE status IN ('PENDING', 'QUEUED', 'IMPORTING')
    `);
    console.log('SOURCE_AVAILABLE_CHAPTER_RECORDS_TOTAL:', totalMappings.rows[0].total_mappings);

    // Check Lançamentos / Release events
    console.log('\n=== 4. LANÇAMENTOS (RECENT RELEASES) ===');
    const recentReleases = await client.query(`
      SELECT c.id, c.work_id, c.number, c.published_at, w.title
      FROM chapters c
      JOIN works w ON w.id = c.work_id
      WHERE c.published_at > NOW() - INTERVAL '15 minutes'
      ORDER BY c.published_at DESC
      LIMIT 10
    `);
    console.log('RECENT PUBLISHED CHAPTERS (LAST 15M):', recentReleases.rows.length);
    console.log('SAMPLE:', recentReleases.rows.slice(0, 3));

    // Check P0 natural events
    console.log('\n=== 5. P0 ABSOLUTO EVENTS ===');
    const p0Events = await client.query(`
      SELECT id, task_type, priority, payload->>'chapterTitle' as title, created_at, updated_at, locked_at
      FROM importer_queue
      WHERE priority >= 100 AND created_at > NOW() - INTERVAL '3 hours'
      ORDER BY created_at DESC
    `);
    console.log('P0 JOBS IN LAST 3H:', p0Events.rows);

  } finally {
    client.release();
    await pool.end();
  }
}

run().catch(console.error);
