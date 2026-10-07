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

async function main() {
  await client.connect();

  console.log('============================================================');
  console.log('1. RUNTIME STATE AUDIT');
  console.log('============================================================');

  // P0_WAITING: Chapters in QUEUED/RETRY with priority >= 100 or isFreshRelease/priority 100
  const p0Res = await client.query(`
    SELECT count(*) as p0_waiting
    FROM importer_queue
    WHERE task_type = 'IMPORT_CHAPTER'
      AND status IN ('QUEUED', 'RETRY')
      AND priority >= 100
  `);
  const p0Waiting = parseInt(p0Res.rows[0]?.p0_waiting || '0', 10);

  // P1_CLAIMABLE: Catalog works (published = true) with chapters in QUEUED/RETRY on active healthy sources
  const p1Res = await client.query(`
    SELECT 
      COUNT(CASE WHEN q.status IN ('QUEUED', 'RETRY') AND (q.next_run_at IS NULL OR q.next_run_at <= NOW()) THEN 1 END) as claimable_cnt,
      COUNT(DISTINCT q.payload->>'workId') as works_cnt
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    JOIN importer_sources s ON s.id = q.source
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND q.status IN ('QUEUED', 'RETRY')
      AND w.published = true
      AND s.enabled = true
      AND s.status = 'ACTIVE'
      AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW());
  `);
  const p1Claimable = parseInt(p1Res.rows[0]?.claimable_cnt || '0', 10);
  const p1WorksCnt = parseInt(p1Res.rows[0]?.works_cnt || '0', 10);

  // Active works from state store
  const stateRes = await client.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
  const activeWorksMap = stateRes.rows[0]?.value || {};
  const activeWorks = Array.isArray(activeWorksMap) ? activeWorksMap : Object.values(activeWorksMap);
  const p1ActiveWorks = activeWorks.filter(w => w.lane === 'P1');
  const p2ActiveWorks = activeWorks.filter(w => w.lane === 'P2');

  // P1_CLAIMED_LAST_10M vs P2_CLAIMED_LAST_10M
  // We identify P1 as published = true, P2 as published = false (admitted new works)
  const claims10mRes = await client.query(`
    SELECT 
      w.published as is_catalog_work,
      q.priority,
      COUNT(*) as cnt
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND q.locked_at >= NOW() - INTERVAL '10 minutes'
    GROUP BY w.published, q.priority;
  `);

  let p1Claimed10m = 0;
  let p2Claimed10m = 0;
  for (const r of claims10mRes.rows) {
    if (r.is_catalog_work) {
      p1Claimed10m += parseInt(r.cnt, 10);
    } else {
      p2Claimed10m += parseInt(r.cnt, 10);
    }
  }

  // P3_WAITING_ADMISSION: Works held in WAITING_ADMISSION
  const p3WaitingRes = await client.query(`
    SELECT count(*) as waiting_cnt
    FROM importer_work_mappings
    WHERE sync_status = 'WAITING_ADMISSION'
  `);
  const p3Waiting = parseInt(p3WaitingRes.rows[0]?.waiting_cnt || '0', 10);

  // P3_ADMITTED_LAST_10M: Works admitted (switched from WAITING_ADMISSION to SYNCED/ACTIVE) or newly admitted in last 10m
  const p3AdmittedRes = await client.query(`
    SELECT count(*) as admitted_cnt
    FROM importer_work_mappings
    WHERE sync_status IN ('SYNCED', 'ACTIVE')
      AND created_at >= NOW() - INTERVAL '10 minutes'
  `);
  const p3Admitted10m = parseInt(p3AdmittedRes.rows[0]?.admitted_cnt || '0', 10);

  // NEW_WORKS_STARTED_LAST_10M: Works created in works table in last 10m
  const newWorksRes = await client.query(`
    SELECT count(*) as new_works_cnt
    FROM works
    WHERE created_at >= NOW() - INTERVAL '10 minutes'
  `);
  const newWorksStarted10m = parseInt(newWorksRes.rows[0]?.new_works_cnt || '0', 10);

  console.log(`P0_WAITING: ${p0Waiting}`);
  console.log(`P1_CLAIMABLE: ${p1Claimable} (across ${p1WorksCnt} catalog works)`);
  console.log(`P1_ACTIVE: ${p1ActiveWorks.length}`);
  console.log(`P1_CLAIMED_LAST_10M: ${p1Claimed10m}`);
  console.log(`P2_ACTIVE_WORKS: ${p2ActiveWorks.length}`);
  console.log(`P2_CLAIMED_LAST_10M: ${p2Claimed10m}`);
  console.log(`P3_WAITING_ADMISSION: ${p3Waiting}`);
  console.log(`P3_ADMITTED_LAST_10M: ${p3Admitted10m}`);
  console.log(`NEW_WORKS_STARTED_LAST_10M: ${newWorksStarted10m}`);

  console.log('\n============================================================');
  console.log('2. LAST 20 CLAIMS AUDIT');
  console.log('============================================================');

  const last20Claims = await client.query(`
    SELECT 
      q.id,
      q.locked_at,
      q.payload->>'workId' as work_id,
      w.title as work_title,
      w.published as is_catalog_work,
      q.chapter_sort_key,
      q.payload->>'chapterNumber' as chapter_number,
      q.priority,
      q.source,
      q.status
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND q.locked_at IS NOT NULL
    ORDER BY q.locked_at DESC
    LIMIT 20
  `);

  const claimsReport = last20Claims.rows.map((r, idx) => {
    let lane = 'P1';
    let why = '';
    if (r.priority >= 100) {
      lane = 'P0';
      why = 'FRESH_RELEASE_PREEMPTION';
    } else if (r.is_catalog_work) {
      lane = 'P1';
      why = 'CATALOG_BACKFILL_CLAIM (published=true)';
    } else {
      lane = 'P2';
      why = 'ACTIVE_NEW_WORK_COHORT';
    }

    return {
      index: idx + 1,
      locked_at: r.locked_at?.toISOString(),
      work: (r.work_title || '').substring(0, 28),
      chapter: r.chapter_number || r.chapter_sort_key,
      source: r.source,
      lane,
      why_selected: why,
      catalog_work: r.is_catalog_work
    };
  });

  console.table(claimsReport);

  await client.end();
}

main().catch(console.error);
