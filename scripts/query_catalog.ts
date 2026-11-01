import { getYugabytePool } from '../src/db/yugabyte-direct.js';

async function main() {
  const pool = getYugabytePool();
  const client = await pool.connect();
  try {
    console.log('--- PROJECT NOX CATÁLOGO & STATUS AUDIT ---');

    // 1. Total works and status
    const worksRes = await client.query(`
      SELECT 
        count(*) as total_works,
        count(*) FILTER (WHERE status = 'COMPLETED') as completed_works,
        count(*) FILTER (WHERE status = 'ONGOING') as ongoing_works,
        count(*) FILTER (WHERE status NOT IN ('COMPLETED', 'ONGOING') OR status IS NULL) as other_status,
        count(*) FILTER (WHERE published = true) as published_works,
        count(*) FILTER (WHERE latest_chapter_published_at IS NOT NULL) as with_latest_chapter_date
      FROM works;
    `);
    console.log('Works Summary:', worksRes.rows[0]);

    // 2. Ghost works check
    const ghostRes = await client.query(`
      SELECT count(*) as ghost_works
      FROM works w
      WHERE w.published = true 
        AND w.latest_chapter_published_at IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM chapters c WHERE c.work_id = w.id);
    `);
    console.log('Ghost Works (must be 0):', ghostRes.rows[0].ghost_works);

    // 3. Works with 0 chapters
    const zeroChapRes = await client.query(`
      SELECT count(*) as works_with_zero_chapters
      FROM works w
      WHERE NOT EXISTS (SELECT 1 FROM chapters c WHERE c.work_id = w.id);
    `);
    console.log('Works with 0 Chapters in DB:', zeroChapRes.rows[0].works_with_zero_chapters);

    // 4. Total chapters
    const chapRes = await client.query(`
      SELECT count(*) as total_chapters
      FROM chapters;
    `);
    console.log('Total Chapters in DB:', chapRes.rows[0].total_chapters);

    // 5. Newest works created
    const newestWorksRes = await client.query(`
      SELECT id, slug, title, created_at, status 
      FROM works 
      ORDER BY created_at DESC 
      LIMIT 5;
    `);
    console.log('Newest Works in DB:');
    console.table(newestWorksRes.rows);

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(err => {
  console.error('Audit failed:', err);
  process.exit(1);
});
