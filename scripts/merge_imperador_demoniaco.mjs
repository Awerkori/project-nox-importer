import pg from 'pg';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const CANONICAL_ID = '122b1026-0f0b-45c5-b8af-ed88a3ea2ce7'; // Imperador Demoníaco
const DUPLICATE_ID = '7ef2aa10-a503-4a19-a184-ba006186ef6a'; // The Servant Is the Demon King?!

async function mergeImperador() {
  const client = new pg.Client({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });
  await client.connect();
  console.log('Connected to YugabyteDB. Preparing safe merge...');

  // Step 1: Generate pre-merge manifest backup for audit & safety (Section 71)
  console.log('Generating pre-merge manifest backup...');
  const canonWork = (await client.query('SELECT * FROM works WHERE id = $1', [CANONICAL_ID])).rows[0];
  const dupWork = (await client.query('SELECT * FROM works WHERE id = $1', [DUPLICATE_ID])).rows[0];

  if (!dupWork) {
    console.log('Duplicate work does not exist or already merged. Verifying canonical work...');
    const verifyCanon = (await client.query('SELECT id, title, slug, published FROM works WHERE id = $1', [CANONICAL_ID])).rows[0];
    console.log('Canonical work status:', verifyCanon);
    await client.end();
    return;
  }

  const canonMappings = (await client.query('SELECT * FROM importer_work_mappings WHERE work_id = $1', [CANONICAL_ID])).rows;
  const dupMappings = (await client.query('SELECT * FROM importer_work_mappings WHERE work_id = $1', [DUPLICATE_ID])).rows;

  const dupChapters = (await client.query('SELECT id, number, title, published_at FROM chapters WHERE work_id = $1 ORDER BY number ASC', [DUPLICATE_ID])).rows;
  const canonChapters = (await client.query('SELECT id, number, title, published_at FROM chapters WHERE work_id = $1 ORDER BY number ASC', [CANONICAL_ID])).rows;

  const dupQueue = (await client.query("SELECT id, status, priority, chapter_sort_key, (payload->>'chapterNumber') as ch_num FROM importer_queue WHERE (payload->>'workId') = $1", [DUPLICATE_ID])).rows;
  const canonQueue = (await client.query("SELECT id, status, priority, chapter_sort_key, (payload->>'chapterNumber') as ch_num FROM importer_queue WHERE (payload->>'workId') = $1", [CANONICAL_ID])).rows;

  const manifest = {
    timestamp: new Date().toISOString(),
    canonicalWorkId: CANONICAL_ID,
    duplicateWorkId: DUPLICATE_ID,
    canonicalBefore: canonWork,
    duplicateBefore: dupWork,
    mappingsBefore: { canonical: canonMappings, duplicate: dupMappings },
    chaptersBefore: { canonicalCount: canonChapters.length, duplicateCount: dupChapters.length },
    queueBefore: { canonicalCount: canonQueue.length, duplicateCount: dupQueue.length }
  };

  const manifestPath = path.resolve(process.cwd(), 'scratch/imperador_merge_manifest.json');
  fs.mkdirSync(path.dirname(manifestPath), { recursive: true });
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
  console.log(`Manifest saved to ${manifestPath}`);

  // Step 2: Atomic Transactional Safe Merge
  console.log('Beginning transaction for atomic safe merge...');
  await client.query('BEGIN;');
  await client.query('SET statement_timeout = 30000;');

  try {
    // 2.1 Update canonical aliases and metadata
    const combinedAliases = Array.from(new Set([
      ...(canonWork.aliases || []),
      ...(dupWork.aliases || []),
      'The Servant Is the Demon King?!',
      'The Servant Is the Demon King',
      'Magic Emperor',
      'Demonic Emperor',
      'Zhuo Yifan'
    ])).filter(Boolean);

    console.log('New canonical aliases:', combinedAliases);

    await client.query(`
      UPDATE works
      SET aliases = $1,
          published = true,
          kind = 'MANHUA',
          latest_chapter_published_at = COALESCE($2, latest_chapter_published_at),
          cover_id = COALESCE(cover_id, $3),
          updated_at = NOW()
      WHERE id = $4
    `, [combinedAliases, dupWork.latest_chapter_published_at, dupWork.cover_id, CANONICAL_ID]);

    // 2.2 Rebind chapters from duplicate to canonical
    console.log(`Rebinding ${dupChapters.length} chapters to canonical work...`);
    const chRebind = await client.query(`
      UPDATE chapters
      SET work_id = $1
      WHERE work_id = $2
      RETURNING id;
    `, [CANONICAL_ID, DUPLICATE_ID]);
    console.log(`Rebound ${chRebind.rowCount} chapters.`);

    // 2.3 Rebind importer_chapter_mappings
    console.log('Rebinding importer_chapter_mappings...');
    const cmRebind = await client.query(`
      UPDATE importer_chapter_mappings
      SET work_id = $1
      WHERE work_id = $2
      RETURNING id;
    `, [CANONICAL_ID, DUPLICATE_ID]);
    console.log(`Rebound ${cmRebind.rowCount} chapter mappings.`);

    // 2.4 Rebind importer_work_mappings
    console.log('Rebinding importer_work_mappings...');
    const wmRebind = await client.query(`
      UPDATE importer_work_mappings
      SET work_id = $1,
          updated_at = NOW()
      WHERE work_id = $2
      RETURNING id;
    `, [CANONICAL_ID, DUPLICATE_ID]);
    console.log(`Rebound ${wmRebind.rowCount} work mappings.`);

    // 2.5 Rebind user relations: library, likes, comments, chapter_views, work_tags
    console.log('Rebinding user and engagement records...');
    await client.query(`
      UPDATE library
      SET work_id = $1
      WHERE work_id = $2
        AND NOT EXISTS (SELECT 1 FROM library l2 WHERE l2.user_id = library.user_id AND l2.work_id = $1)
    `, [CANONICAL_ID, DUPLICATE_ID]);
    await client.query(`DELETE FROM library WHERE work_id = $1`, [DUPLICATE_ID]);

    await client.query(`
      UPDATE likes
      SET work_id = $1
      WHERE work_id = $2
        AND NOT EXISTS (SELECT 1 FROM likes l2 WHERE l2.user_id = likes.user_id AND l2.work_id = $1)
    `, [CANONICAL_ID, DUPLICATE_ID]);
    await client.query(`DELETE FROM likes WHERE work_id = $1`, [DUPLICATE_ID]);

    await client.query(`
      UPDATE comments SET work_id = $1 WHERE work_id = $2
    `, [CANONICAL_ID, DUPLICATE_ID]);

    await client.query(`
      UPDATE chapter_views SET work_id = $1 WHERE work_id = $2
    `, [CANONICAL_ID, DUPLICATE_ID]);

    await client.query(`
      INSERT INTO work_tags (work_id, tag_id, system_generated)
      SELECT $1, tag_id, system_generated
      FROM work_tags
      WHERE work_id = $2
      ON CONFLICT (work_id, tag_id) DO NOTHING
    `, [CANONICAL_ID, DUPLICATE_ID]);
    await client.query(`DELETE FROM work_tags WHERE work_id = $1`, [DUPLICATE_ID]);

    // 2.6 Rebind queue jobs and supersede redundant published chapters
    console.log('Reconciling queue jobs...');
    // A. Rebind duplicate jobs' payload workId to canonical
    await client.query(`
      UPDATE importer_queue
      SET payload = jsonb_set(payload::jsonb, '{workId}', to_jsonb($1::text)),
          updated_at = NOW()
      WHERE (payload->>'workId') = $2;
    `, [CANONICAL_ID, DUPLICATE_ID]);

    // B. For any jobs that correspond to chapters ALREADY published in canonical work, mark SUPERSEDED
    const pubChapters = (await client.query('SELECT number FROM chapters WHERE work_id = $1', [CANONICAL_ID])).rows;
    const pubNumbers = pubChapters.map(r => parseFloat(r.number));
    console.log(`Canonical has ${pubNumbers.length} published chapters. Superseding redundant pending jobs...`);

    const superRes = await client.query(`
      UPDATE importer_queue
      SET status = 'SUPERSEDED',
          locked_by = NULL,
          locked_at = NULL,
          lease_expires_at = NULL,
          updated_at = NOW()
      WHERE (payload->>'workId') = $1
        AND status IN ('QUEUED', 'PAUSED_BY_STAFF', 'RETRY')
        AND chapter_sort_key = ANY($2::numeric[])
      RETURNING id;
    `, [CANONICAL_ID, pubNumbers]);
    console.log(`Superseded ${superRes.rowCount} redundant queue jobs for already-published chapters.`);

    // 2.7 Deprecate duplicate work safely (do not delete hard, keep tombstone / unpublish)
    console.log('Deprecating duplicate work record...');
    await client.query(`
      UPDATE works
      SET published = false,
          slug = 'the-servant-is-the-demon-king-deprecated',
          title = 'The Servant Is the Demon King?! [MERGED]',
          synopsis = '[MERGED INTO CANONICAL WORK: ' || $1 || ']',
          description = '[MERGED INTO CANONICAL WORK: ' || $1 || ']',
          updated_at = NOW()
      WHERE id = $2;
    `, [CANONICAL_ID, DUPLICATE_ID]);

    await client.query('COMMIT;');
    console.log('🎉 TRANSACTION COMMITTED SUCCESSFULLY!');
  } catch (err) {
    console.error('Error during merge, rolling back:', err);
    await client.query('ROLLBACK;');
    throw err;
  }

  // Step 3: Verification post-merge
  console.log('\n--- POST-MERGE VERIFICATION ---');
  const postCanon = (await client.query('SELECT id, title, slug, published, aliases FROM works WHERE id = $1', [CANONICAL_ID])).rows[0];
  const postDup = (await client.query('SELECT id, title, slug, published FROM works WHERE id = $1', [DUPLICATE_ID])).rows[0];
  const postChapters = (await client.query('SELECT count(*) as cnt, min(number) as min_num, max(number) as max_num FROM chapters WHERE work_id = $1', [CANONICAL_ID])).rows[0];
  const postMappings = (await client.query('SELECT source, source_work_id, source_title FROM importer_work_mappings WHERE work_id = $1', [CANONICAL_ID])).rows;
  const postQueue = (await client.query("SELECT status, count(*) FROM importer_queue WHERE (payload->>'workId') = $1 GROUP BY status", [CANONICAL_ID])).rows;

  console.log('Canonical work:', postCanon);
  console.log('Duplicate work:', postDup);
  console.log('Canonical chapters:', postChapters);
  console.log('Canonical mappings:', postMappings);
  console.log('Canonical queue breakdown:', postQueue);

  await client.end();
}

mergeImperador().catch(err => {
  console.error('Fatal merge failure:', err);
  process.exit(1);
});
