import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function run() {
  await client.connect();
  const wid = '23354114-053e-4d2b-98f3-31cbc0744161'; // Gênio do Teletransporte

  // Query staged chapters for this work ordered by chapter_sort_key
  const staged = await client.query(`
    SELECT m.chapter_id, m.chapter_number, m.chapter_sort_key
    FROM importer_chapter_mappings m
    WHERE m.work_id = $1::uuid AND m.status = 'STAGED'
    ORDER BY m.chapter_sort_key ASC;
  `, [wid]);

  console.log(`Found ${staged.rows.length} staged chapters for Gênio`);

  let publishedCount = 0;
  for (const ch of staged.rows) {
    const sortKey = parseFloat(ch.chapter_sort_key);

    // Get max published
    const maxPubRes = await client.query(`
      SELECT COALESCE(MAX(number), -1) as max_pub 
      FROM chapters 
      WHERE work_id = $1::uuid AND published_at IS NOT NULL;
    `, [wid]);
    const maxPub = parseFloat(maxPubRes.rows[0]?.max_pub ?? '-1');

    const step = sortKey - maxPub;
    // Check if gap exists
    if (step > 1.5) {
      const gapRes = await client.query(`
        SELECT COUNT(*) as gap_count 
        FROM importer_chapter_mappings 
        WHERE work_id = $1::uuid AND chapter_sort_key > $2 AND chapter_sort_key < $3 AND is_gap IS TRUE;
      `, [wid, maxPub, sortKey]);
      const gapCount = parseInt(gapRes.rows[0]?.gap_count || '0', 10);
      if (gapCount === 0) {
        console.log(`Gap blocked between ${maxPub} and ${sortKey}`);
        break;
      }
    }

    // Publish!
    await client.query(`
      UPDATE chapters 
      SET published_at = NOW() 
      WHERE id = $1::uuid AND published_at IS NULL;
    `, [ch.chapter_id]);

    await client.query(`
      UPDATE importer_chapter_mappings 
      SET status = 'COMPLETED', updated_at = NOW() 
      WHERE chapter_id = $1::uuid;
    `, [ch.chapter_id]);

    publishedCount++;
  }

  console.log(`Successfully cascaded and published ${publishedCount} chapters for Gênio do Teletransporte!`);

  // Update latest_chapter_published_at on works
  const latestPub = await client.query(`
    SELECT MAX(published_at) as latest 
    FROM chapters 
    WHERE work_id = $1::uuid AND published_at IS NOT NULL;
  `, [wid]);
  if (latestPub.rows[0]?.latest) {
    await client.query(`
      UPDATE works 
      SET published = true, latest_chapter_published_at = $2, updated_at = NOW() 
      WHERE id = $1::uuid;
    `, [wid, latestPub.rows[0].latest]);
  }

  await client.end();
}
run().catch(console.error);
