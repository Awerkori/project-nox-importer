import pg from 'pg';
import fs from 'node:fs';
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

  const progressPath = '/home/awerkori/.Projects/project-nox-importer/validation_30m_live_progress.json';
  let progress = {};
  if (fs.existsSync(progressPath)) {
    progress = JSON.parse(fs.readFileSync(progressPath, 'utf8'));
  }
  const startIso = progress.startIso || '2026-09-21T20:56:50.240Z';

  // 1. Staged dynamics
  const stagedDynamics = await client.query(`
    SELECT 
      (SELECT count(*) FROM importer_chapter_mappings WHERE created_at >= $1) as entered_staged,
      (SELECT count(*) FROM importer_chapter_mappings WHERE updated_at >= $1 AND status != 'STAGED') as left_staged,
      (SELECT count(*) FROM chapters WHERE published_at >= $1) as published_from_staged
  `, [startIso]);

  // 2. Duplicate jobs audit
  const dupAudit = await client.query(`
    SELECT 
      count(*) FILTER (WHERE last_error = 'CANONICAL_ALREADY_SATISFIED' AND updated_at >= $1) as short_circuited,
      count(*) FILTER (WHERE status = 'COMPLETED' AND updated_at >= $1) as total_completed
    FROM importer_queue
  `, [startIso]);

  // 3. Gap blocked & Upstream blocked
  const queueState = await client.query(`
    SELECT 
      count(*) FILTER (WHERE status = 'QUEUED') as queued_total,
      count(*) FILTER (WHERE status = 'BLOCKED_BY_UPSTREAM') as upstream_blocked,
      count(*) FILTER (WHERE status = 'PAUSED_BY_STAFF') as paused_by_staff
    FROM importer_queue
  `);

  // 4. Inflight workers and reasons for idle slots
  const inflightRes = await client.query(`
    SELECT (payload->>'workId') as work_id, count(*) as cnt
    FROM importer_queue
    WHERE status = 'IMPORTING'
    GROUP BY (payload->>'workId')
  `);
  const activeWorkers = inflightRes.rows.reduce((a, b) => a + parseInt(b.cnt, 10), 0);
  const worksWithMaxInflight = inflightRes.rows.filter(r => parseInt(r.cnt, 10) >= 2).length;

  // 5. Active works in scheduler
  const stateRes = await client.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
  const activeWorksMap = stateRes.rows[0]?.value || {};
  const activeWorks = Array.isArray(activeWorksMap) ? activeWorksMap : Object.values(activeWorksMap);
  const fillingP1 = activeWorks.filter(w => w.lane === 'P1' && w.state === 'FILLING');
  const fillingP2 = activeWorks.filter(w => w.lane === 'P2' && w.state === 'FILLING');

  // Claimable jobs in active works
  const fillingIds = [...fillingP1, ...fillingP2].map(w => w.workId);
  let claimableInActive = 0;
  if (fillingIds.length > 0) {
    const qInActive = await client.query(`
      SELECT count(*) as cnt 
      FROM importer_queue 
      WHERE status = 'QUEUED' AND (payload->>'workId') = ANY($1::text[])
    `, [fillingIds]);
    claimableInActive = parseInt(qInActive.rows[0].cnt, 10);
  }

  // 6. Check for duplicate canonical chapters or works created since start
  const dupChapters = await client.query(`
    SELECT work_id, number, count(*)
    FROM chapters
    WHERE published_at >= $1
    GROUP BY work_id, number
    HAVING count(*) > 1
  `, [startIso]);

  const dupWorks = await client.query(`
    SELECT slug, count(*)
    FROM works
    WHERE created_at >= $1
    GROUP BY slug
    HAVING count(*) > 1
  `, [startIso]);

  console.log(JSON.stringify({
    progress,
    stagedDynamics: stagedDynamics.rows[0],
    dupAudit: dupAudit.rows[0],
    queueState: queueState.rows[0],
    activeWorkers,
    idleWorkers: Math.max(0, 18 - activeWorkers),
    worksWithMaxInflight,
    fillingP1Count: fillingP1.length,
    fillingP2Count: fillingP2.length,
    claimableInActive,
    newDuplicateChapters: dupChapters.rows.length,
    newDuplicateWorks: dupWorks.rows.length
  }, null, 2));

  await client.end();
}
run().catch(console.error);
