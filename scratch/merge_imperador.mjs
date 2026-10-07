import dotenv from 'dotenv';
import pg from 'pg';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;

async function run() {
  const isDryRun = process.argv.includes('--dry-run');
  console.log(`Running merge_imperador.mjs (DRY RUN = ${isDryRun})...\n`);

  const client = new Client({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  await client.connect();

  const canonicalId = '4b1f452c-b223-404c-83f4-e0d626563397'; // Imperador Mágico
  const duplicateId = '122b1026-0f0b-45c5-b8af-ed88a3ea2ce7'; // Imperador Demoníaco

  // 1. Fetch works
  const [wCanon, wDup] = await Promise.all([
    client.query('SELECT * FROM works WHERE id = $1', [canonicalId]),
    client.query('SELECT * FROM works WHERE id = $1', [duplicateId])
  ]);

  if (!wCanon.rows[0]) throw new Error('Canonical work not found!');
  if (!wDup.rows[0]) throw new Error('Duplicate work not found!');

  console.log(`Found Canonical: "${wCanon.rows[0].title}" (${canonicalId})`);
  console.log(`Found Duplicate: "${wDup.rows[0].title}" (${duplicateId})`);

  // 2. Chapters mapping & analysis
  const [chCanon, chDup] = await Promise.all([
    client.query('SELECT id, number, title, published_at FROM chapters WHERE work_id = $1', [canonicalId]),
    client.query('SELECT id, number, title, published_at FROM chapters WHERE work_id = $1', [duplicateId])
  ]);

  const canonByNumber = new Map();
  for (const c of chCanon.rows) {
    canonByNumber.set(String(c.number), c);
  }

  const toMigrateUnique = [];
  const toRebindOverlapping = [];

  for (const d of chDup.rows) {
    const numStr = String(d.number);
    if (canonByNumber.has(numStr)) {
      toRebindOverlapping.push({
        dupChapterId: d.id,
        canonChapterId: canonByNumber.get(numStr).id,
        number: numStr
      });
    } else {
      toMigrateUnique.push(d);
    }
  }

  console.log(`\nChapters analysis:`);
  console.log(`Canonical total chapters: ${chCanon.rows.length}`);
  console.log(`Duplicate total chapters: ${chDup.rows.length}`);
  console.log(`Overlapping chapters to rebind: ${toRebindOverlapping.length}`);
  console.log(`Unique chapters to migrate to canonical: ${toMigrateUnique.length} (${toMigrateUnique.map(c => c.number).join(', ')})`);

  // 3. Work mappings to move
  const mapDup = await client.query('SELECT * FROM importer_work_mappings WHERE work_id = $1', [duplicateId]);
  console.log(`\nWork mappings to rebind: ${mapDup.rows.length} (${mapDup.rows.map(m => m.source).join(', ')})`);

  // 4. Aliases to add to canonical
  const existingAliases = new Set(wCanon.rows[0].aliases || []);
  const candidateAliases = [
    'Imperador Demoníaco',
    'The Servant Is the Demon King?!',
    'Magic Emperor',
    'Demonic Emperor',
    'Devil\'s Butler',
    'Mo Huang Da Guan Jia'
  ];
  const newAliases = [...existingAliases];
  for (const a of candidateAliases) {
    if (!existingAliases.has(a)) {
      newAliases.push(a);
    }
  }

  if (isDryRun) {
    console.log('\n[DRY RUN COMPLETE] No modifications made.');
    await client.end();
    return;
  }

  // EXECUTE TRANSACTION
  await client.query('BEGIN');
  try {
    // A. Rebind work mappings
    const wmRes = await client.query(
      'UPDATE importer_work_mappings SET work_id = $1, updated_at = NOW() WHERE work_id = $2',
      [canonicalId, duplicateId]
    );
    console.log(`[EXEC] Updated ${wmRes.rowCount} work mappings to point to canonical.`);

    // B. Rebind overlapping chapter mappings using fast batch
    if (toRebindOverlapping.length > 0) {
      const valuesSql = toRebindOverlapping
        .map((_, idx) => `($${idx * 2 + 3}::uuid, $${idx * 2 + 4}::uuid)`)
        .join(', ');
      const params = [canonicalId, duplicateId];
      for (const item of toRebindOverlapping) {
        params.push(item.canonChapterId, item.dupChapterId);
      }

      const batchQuery = `
        UPDATE importer_chapter_mappings AS m
        SET chapter_id = v.canon_id, work_id = $1, updated_at = NOW()
        FROM (VALUES ${valuesSql}) AS v(canon_id, dup_id)
        WHERE m.work_id = $2 AND m.chapter_id = v.dup_id;
      `;
      const chMapRes = await client.query(batchQuery, params);
      console.log(`[EXEC] Rebound ${chMapRes.rowCount} chapter mapping records to canonical chapters in 1 batch query.`);
    }

    // Rebind any remaining chapter mappings pointing to duplicate work_id
    const remChMap = await client.query(
      'UPDATE importer_chapter_mappings SET work_id = $1, updated_at = NOW() WHERE work_id = $2',
      [canonicalId, duplicateId]
    );
    console.log(`[EXEC] Rebound ${remChMap.rowCount} remaining chapter mapping work_id references.`);

    // C. Migrate unique chapters from duplicate work to canonical work (table chapters has no updated_at)
    if (toMigrateUnique.length > 0) {
      const uniqueIds = toMigrateUnique.map(u => u.id);
      const migRes = await client.query(
        'UPDATE chapters SET work_id = $1 WHERE id = ANY($2::uuid[])',
        [canonicalId, uniqueIds]
      );
      console.log(`[EXEC] Migrated ${migRes.rowCount} unique chapters to canonical work.`);
    }

    // D. Safely delete the redundant overlapping chapters from duplicate work
    const dupIdsToDelete = toRebindOverlapping.map(i => i.dupChapterId);
    if (dupIdsToDelete.length > 0) {
      const pDel = await client.query('DELETE FROM pages WHERE chapter_id = ANY($1::uuid[])', [dupIdsToDelete]);
      console.log(`[EXEC] Cleaned up ${pDel.rowCount} duplicate page records.`);
      
      const chDel = await client.query('DELETE FROM chapters WHERE id = ANY($1::uuid[])', [dupIdsToDelete]);
      console.log(`[EXEC] Deleted ${chDel.rowCount} redundant chapter records.`);
    }

    // E. Rebind importer_job_metrics
    const jmRes = await client.query('UPDATE importer_job_metrics SET work_id = $1 WHERE work_id = $2', [canonicalId, duplicateId]);
    console.log(`[EXEC] Rebound ${jmRes.rowCount} job metrics.`);

    // F. Rebind work_tags if any
    try {
      const curTags = await client.query('SELECT tag_id FROM work_tags WHERE work_id = $1', [canonicalId]);
      const curTagIds = new Set(curTags.rows.map(r => r.tag_id));
      const dupTags = await client.query('SELECT tag_id FROM work_tags WHERE work_id = $1', [duplicateId]);
      for (const t of dupTags.rows) {
        if (!curTagIds.has(t.tag_id)) {
          await client.query('INSERT INTO work_tags (work_id, tag_id) VALUES ($1, $2) ON CONFLICT DO NOTHING', [canonicalId, t.tag_id]);
        }
      }
      await client.query('DELETE FROM work_tags WHERE work_id = $1', [duplicateId]);
      console.log(`[EXEC] Merged and cleaned work_tags.`);
    } catch (e) {
      console.log(`[INFO] work_tags merge skipped or not present:`, e.message);
    }

    // G. Update canonical work aliases
    await client.query('UPDATE works SET aliases = $1, updated_at = NOW() WHERE id = $2', [newAliases, canonicalId]);
    console.log(`[EXEC] Updated canonical work aliases.`);

    // H. Delete the duplicate work record (all references removed)
    await client.query('DELETE FROM works WHERE id = $1', [duplicateId]);
    console.log(`[EXEC] Deleted duplicate work record (${duplicateId}).`);

    await client.query('COMMIT');
    console.log('\n🎉 [SUCCESS] Imperador Demoníaco successfully and safely merged into Imperador Mágico!');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('\n❌ [ROLLBACK] Transaction rolled back due to error:', err);
    throw err;
  } finally {
    await client.end();
  }
}

run().catch(console.error);
