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

  // 1. Zero Duplicate Works in DB
  const dupes = await client.query(`
    SELECT lower(trim(title)) as norm_title, count(*) as cnt
    FROM works
    GROUP BY lower(trim(title))
    HAVING count(*) > 1
  `);
  console.log(`\n[CRITERIA 1] Zero duplicate works in DB: ${dupes.rows.length === 0 ? 'PASSED (0 duplicates)' : 'FAILED (' + dupes.rows.length + ' duplicates)'}`);
  if (dupes.rows.length > 0) {
    console.log(dupes.rows);
  }

  // 2. Imperador Demoníaco Status
  const imp = await client.query(`
    SELECT id, title, slug, published,
           (SELECT count(*) FROM chapters WHERE work_id = works.id) as chapter_count
    FROM works
    WHERE id = '122b1026-0f0b-45c5-b8af-ed88a3ea2ce7'
  `);
  console.log('\n[CRITERIA 2] Canonical Imperador Demoníaco:');
  console.log(imp.rows[0]);

  const impQueue = await client.query(`
    SELECT status, priority, count(*)
    FROM importer_queue
    WHERE (payload->>'workId') = '122b1026-0f0b-45c5-b8af-ed88a3ea2ce7'
    GROUP BY status, priority
    ORDER BY priority DESC, status
  `);
  console.log('  Imperador Demoníaco Queue Jobs:', impQueue.rows);

  // 3. Strict Priority Policy (P0 > P1 > P2 > P3)
  const activeQueued = await client.query(`
    SELECT q.priority, 
           count(*) as count,
           count(*) FILTER (WHERE w.published = true) as published_backfill,
           count(*) FILTER (WHERE w.published = false) as new_works
    FROM importer_queue q
    LEFT JOIN works w ON w.id = (q.payload->>'workId')::uuid
    WHERE q.status IN ('QUEUED', 'IMPORTING')
    GROUP BY q.priority
    ORDER BY q.priority DESC
  `);
  console.log('\n[CRITERIA 3] Active/Queued Jobs by Priority:');
  console.table(activeQueued.rows);

  // 4. Cohort limit (<= 4 active new works)
  const activeNewWorks = await client.query(`
    SELECT (q.payload->>'workId') as work_id, w.title, count(*) as queued_jobs
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    WHERE q.status IN ('QUEUED', 'IMPORTING') AND w.published = false
    GROUP BY q.payload->>'workId', w.title
  `);
  console.log(`\n[CRITERIA 4] Active New Works (P2 cohort): ${activeNewWorks.rows.length} works (Limit <= 4):`);
  console.table(activeNewWorks.rows);

  // 5. Sequence continuity (No jumps/skips)
  const recentChaps = await client.query(`
    SELECT work_id, number, published_at
    FROM chapters
    WHERE published_at >= NOW() - INTERVAL '30 minutes'
    ORDER BY work_id, number ASC
  `);
  console.log(`\n[CRITERIA 5] Chapters published in last 30 minutes: ${recentChaps.rows.length}`);
  
  let jumpsDetected = 0;
  for (const r of recentChaps.rows) {
    const num = parseFloat(r.number);
    if (num <= 1) continue; // First chapter / prologue
    
    // Verify preceding chapter exists in chapters table
    const prev = await client.query(`
      SELECT count(*) as count
      FROM chapters
      WHERE work_id = $1 AND number >= $2 AND number < $3
    `, [r.work_id, num - 1, num]);

    if (parseInt(prev.rows[0].count, 10) === 0) {
      console.warn(`⚠️ Unbacked sequence jump on work ${r.work_id}: chapter ${num} has no chapter in [${num-1}, ${num})`);
      jumpsDetected++;
    }
  }
  console.log(`  Sequence jumps detected: ${jumpsDetected}`);

  // 6. Settings status
  const settings = await client.query(`SELECT key, value FROM settings WHERE key IN ('importer_protective_stop', 'publication_safety_barrier', 'catalog_discovery_enabled', 'work_affinity_scheduler_enabled')`);
  console.log('\n[CRITERIA 6] Settings:');
  for (const s of settings.rows) {
    console.log(`  ${s.key}: ${s.value}`);
  }

  await client.end();
}

main().catch(err => {
  console.error('Error during verification:', err);
  process.exit(1);
});
