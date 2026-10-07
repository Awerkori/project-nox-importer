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
  console.log('Connected to YugabyteDB production');

  // =========================================================================
  // STEP 1: Clean up "Entrar na Conta" bogus scraping artifacts
  // =========================================================================
  console.log('\n--- STEP 1: Cleaning up "Entrar na Conta" artifacts ---');
  const encWorks = await client.query(`
    SELECT id, slug FROM works WHERE lower(trim(title)) = 'entrar na conta'
  `);
  if (encWorks.rows.length > 0) {
    const encIds = encWorks.rows.map(r => r.id);
    console.log(`Found ${encIds.length} 'Entrar na Conta' works. Removing queue jobs and mappings...`);

    const qDel = await client.query(`
      DELETE FROM importer_queue WHERE (payload->>'workId') = ANY($1::text[])
    `, [encIds]);
    console.log(`Deleted ${qDel.rowCount} queue jobs for 'Entrar na Conta'`);

    const cmDel = await client.query(`
      DELETE FROM importer_chapter_mappings WHERE work_id = ANY($1::uuid[])
    `, [encIds]);
    console.log(`Deleted ${cmDel.rowCount} chapter mappings for 'Entrar na Conta'`);

    const wmDel = await client.query(`
      DELETE FROM importer_work_mappings WHERE work_id = ANY($1::uuid[])
    `, [encIds]);
    console.log(`Deleted ${wmDel.rowCount} work mappings for 'Entrar na Conta'`);

    const wDel = await client.query(`
      DELETE FROM works WHERE id = ANY($1::uuid[])
    `, [encIds]);
    console.log(`Deleted ${wDel.rowCount} works for 'Entrar na Conta'`);
  } else {
    console.log('No "Entrar na Conta" works found.');
  }

  // =========================================================================
  // STEP 2: Canonical Deduplication of duplicate works
  // =========================================================================
  console.log('\n--- STEP 2: Canonical Deduplication of Duplicate Works ---');
  const dupeGroups = await client.query(`
    SELECT lower(trim(title)) as norm_title, array_agg(id::text) as ids
    FROM works
    GROUP BY lower(trim(title))
    HAVING count(*) > 1
    ORDER BY lower(trim(title))
  `);

  console.log(`Found ${dupeGroups.rows.length} duplicate title groups to merge.`);

  for (const group of dupeGroups.rows) {
    const worksList = [];
    for (const wid of group.ids) {
      const wRes = await client.query('SELECT id, title, slug, published, created_at FROM works WHERE id = $1::uuid', [wid]);
      if (wRes.rows.length === 0) continue;
      const w = wRes.rows[0];
      const chRes = await client.query('SELECT count(*)::int as count FROM chapters WHERE work_id = $1::uuid', [wid]);
      worksList.push({
        ...w,
        chapterCount: chRes.rows[0].count,
      });
    }

    if (worksList.length <= 1) continue;

    // Sort to determine canonical work:
    // 1. Published works first
    // 2. Highest chapter count
    // 3. Slugs that don't end in -2, -3
    // 4. Oldest creation date
    worksList.sort((a, b) => {
      if (a.published !== b.published) return a.published ? -1 : 1;
      if (a.chapterCount !== b.chapterCount) return b.chapterCount - a.chapterCount;
      const aSuff = /-\d+$/.test(a.slug);
      const bSuff = /-\d+$/.test(b.slug);
      if (aSuff !== bSuff) return aSuff ? 1 : -1;
      return new Date(a.created_at).getTime() - new Date(b.created_at).getTime();
    });

    const canonical = worksList[0];
    const duplicates = worksList.slice(1);

    console.log(`\nMerging "${group.norm_title}":`);
    console.log(`  CANONICAL: ${canonical.id} (${canonical.slug}) [pub:${canonical.published}, ch:${canonical.chapterCount}]`);

    for (const dupe of duplicates) {
      console.log(`  DUPLICATE: ${dupe.id} (${dupe.slug}) [pub:${dupe.published}, ch:${dupe.chapterCount}]`);

      // 1. Handle chapters on duplicate
      const dupeChapters = await client.query('SELECT id, number FROM chapters WHERE work_id = $1::uuid', [dupe.id]);
      for (const ch of dupeChapters.rows) {
        const exist = await client.query('SELECT id FROM chapters WHERE work_id = $1::uuid AND number = $2', [canonical.id, ch.number]);
        if (exist.rows.length > 0) {
          // Canonical already has this chapter: delete duplicate's chapter
          await client.query('DELETE FROM pages WHERE chapter_id = $1::uuid', [ch.id]);
          await client.query('DELETE FROM chapters WHERE id = $1::uuid', [ch.id]);
        } else {
          // Canonical missing this chapter: move chapter to canonical
          await client.query('UPDATE chapters SET work_id = $1::uuid WHERE id = $2::uuid', [canonical.id, ch.id]);
        }
      }

      // 2. Handle work mappings
      const dupeWorkMaps = await client.query('SELECT id, source, source_work_id FROM importer_work_mappings WHERE work_id = $1::uuid', [dupe.id]);
      for (const wm of dupeWorkMaps.rows) {
        const existWm = await client.query('SELECT id FROM importer_work_mappings WHERE source = $1 AND source_work_id = $2', [wm.source, wm.source_work_id]);
        if (existWm.rows.length > 0 && existWm.rows[0].id !== wm.id) {
          await client.query('DELETE FROM importer_work_mappings WHERE id = $1::uuid', [wm.id]);
        } else {
          await client.query('UPDATE importer_work_mappings SET work_id = $1::uuid WHERE id = $2::uuid', [canonical.id, wm.id]);
        }
      }

      // 3. Handle chapter mappings
      const dupeChapMaps = await client.query('SELECT id, source, source_chapter_id FROM importer_chapter_mappings WHERE work_id = $1::uuid', [dupe.id]);
      for (const cm of dupeChapMaps.rows) {
        const existCm = await client.query('SELECT id FROM importer_chapter_mappings WHERE source = $1 AND source_chapter_id = $2', [cm.source, cm.source_chapter_id]);
        if (existCm.rows.length > 0 && existCm.rows[0].id !== cm.id) {
          await client.query('DELETE FROM importer_chapter_mappings WHERE id = $1::uuid', [cm.id]);
        } else {
          await client.query('UPDATE importer_chapter_mappings SET work_id = $1::uuid WHERE id = $2::uuid', [canonical.id, cm.id]);
        }
      }

      // 4. Handle queue jobs
      await client.query(`
        UPDATE importer_queue 
        SET payload = jsonb_set(payload, '{workId}', to_jsonb($1::text))
        WHERE (payload->>'workId') = $2
      `, [canonical.id, dupe.id]);

      // 5. Delete duplicate work
      await client.query('DELETE FROM works WHERE id = $1::uuid', [dupe.id]);
      console.log(`  Merged and deleted duplicate ${dupe.id}`);
    }
  }

  // =========================================================================
  // STEP 3: Priority Realignment & Cohort Enforcement
  // =========================================================================
  console.log('\n--- STEP 3: Priority Realignment & Cohort Enforcement ---');

  // 1. Published works backfill -> Priority 75 (P1)
  const p1Update = await client.query(`
    UPDATE importer_queue q
    SET priority = 75
    FROM works w
    WHERE (q.payload->>'workId')::uuid = w.id
      AND w.published = true
      AND q.task_type = 'IMPORT_CHAPTER'
      AND q.status IN ('QUEUED', 'RETRY')
      AND (q.payload->>'isFreshRelease')::boolean IS NOT TRUE
      AND q.priority != 75
  `);
  console.log(`Updated ${p1Update.rowCount} published backfill jobs to priority 75 (P1)`);

  // 2. Identify top 4 active new works for cohort (P2 = 50)
  // Find unpublished works with pending/queued jobs
  const unpubWorks = await client.query(`
    SELECT (q.payload->>'workId') as work_id, w.title, count(*)::int as queued_count
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    WHERE w.published = false
      AND q.task_type = 'IMPORT_CHAPTER'
      AND q.status IN ('QUEUED', 'PAUSED_BY_STAFF', 'WAITING_ADMISSION')
    GROUP BY q.payload->>'workId', w.title
    ORDER BY queued_count DESC
  `);

  console.log(`Found ${unpubWorks.rows.length} total unpublished works with chapters.`);
  const admittedCohort = unpubWorks.rows.slice(0, 4);
  const admittedIds = admittedCohort.map(r => r.work_id);
  console.log(`Admitted cohort of 4 works:`, admittedCohort.map(r => `${r.title} (${r.work_id})`));

  // For the 4 admitted works: admit sliding window (first 8 chapters) to QUEUED with priority 50 (P2),
  // leave later chapters parked in PAUSED_BY_STAFF
  for (const adm of admittedCohort) {
    // Get all jobs for this work ordered by chapterSortKey
    const jobs = await client.query(`
      SELECT id, chapter_sort_key 
      FROM importer_queue 
      WHERE (payload->>'workId') = $1 AND task_type = 'IMPORT_CHAPTER'
      ORDER BY chapter_sort_key ASC NULLS LAST
    `, [adm.work_id]);

    const activeWindow = jobs.rows.slice(0, 8).map(j => j.id);
    const parkedRest = jobs.rows.slice(8).map(j => j.id);

    if (activeWindow.length > 0) {
      await client.query(`
        UPDATE importer_queue
        SET status = 'QUEUED', priority = 50
        WHERE id = ANY($1::uuid[]) AND status != 'COMPLETED'
      `, [activeWindow]);
    }
    if (parkedRest.length > 0) {
      await client.query(`
        UPDATE importer_queue
        SET status = 'PAUSED_BY_STAFF'
        WHERE id = ANY($1::uuid[]) AND status != 'COMPLETED'
      `, [parkedRest]);
    }
  }

  // For all OTHER unpublished works (not in the 4 admitted cohort):
  // Park their chapters in PAUSED_BY_STAFF so they do not consume worker slots
  const nonAdmittedIds = unpubWorks.rows.slice(4).map(r => r.work_id);
  if (nonAdmittedIds.length > 0) {
    const parkOther = await client.query(`
      UPDATE importer_queue
      SET status = 'PAUSED_BY_STAFF', priority = 50
      WHERE (payload->>'workId') = ANY($1::text[])
        AND task_type = 'IMPORT_CHAPTER'
        AND status IN ('QUEUED', 'RETRY')
    `, [nonAdmittedIds]);
    console.log(`Parked ${parkOther.rowCount} queued chapter jobs from non-admitted works into PAUSED_BY_STAFF`);
  }

  // =========================================================================
  // STEP 4: Reset importer_protective_stop in settings
  // =========================================================================
  console.log('\n--- STEP 4: Resetting importer_protective_stop ---');
  await client.query(`
    UPDATE settings 
    SET value = '{"active":false,"reason":"Manual reset after full audit & priority alignment","cleared_at":"${new Date().toISOString()}"}'
    WHERE key = 'importer_protective_stop'
  `);
  console.log('Reset importer_protective_stop to inactive.');

  // =========================================================================
  // STEP 5: Verify post-cleanup state
  // =========================================================================
  console.log('\n--- STEP 5: Verifying Clean State ---');
  const remainingDupes = await client.query(`
    SELECT lower(trim(title)) as norm_title, count(*)
    FROM works
    GROUP BY lower(trim(title))
    HAVING count(*) > 1
  `);
  console.log(`Remaining duplicate titles in DB: ${remainingDupes.rows.length}`);

  const activeNewWorks = await client.query(`
    SELECT count(DISTINCT (q.payload->>'workId'))::int as active_new_works
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    WHERE q.status = 'QUEUED' AND w.published = false
  `);
  console.log(`Active new works with QUEUED jobs: ${activeNewWorks.rows[0].active_new_works} (limit <= 4)`);

  const queueSummary = await client.query(`
    SELECT priority, status, count(*)::int as cnt
    FROM importer_queue
    WHERE status IN ('QUEUED', 'IMPORTING', 'RETRY')
    GROUP BY priority, status
    ORDER BY priority DESC, status
  `);
  console.log('Active queue jobs breakdown:');
  for (const r of queueSummary.rows) {
    console.log(` - Priority ${r.priority} | ${r.status}: ${r.cnt} jobs`);
  }

  await client.end();
  console.log('\nDatabase cleanup and priority realignment completed successfully!');
}

main().catch(err => {
  console.error('Fatal error during cleanup:', err);
  process.exit(1);
});
