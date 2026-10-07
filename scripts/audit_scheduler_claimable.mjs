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

  // 1. Total QUEUED vs statuses
  const statusRes = await client.query(`
    SELECT status, count(*) 
    FROM importer_queue 
    GROUP BY status
  `);
  console.log("=== QUEUE STATUS COUNTS ===");
  console.table(statusRes.rows);

  // 2. Active works in scheduler state
  const stateRes = await client.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
  const activeWorksMap = stateRes.rows[0]?.value || {};
  const activeWorks = Array.isArray(activeWorksMap) ? activeWorksMap : Object.values(activeWorksMap);
  console.log(`=== ACTIVE WORKS IN SCHEDULER: ${activeWorks.length} ===`);
  const fillingWorks = activeWorks.filter(w => w.state === 'FILLING');
  console.log(`FILLING active works: ${fillingWorks.length}`);
  const activeWorkIds = fillingWorks.map(w => w.workId);

  // 3. In-flight jobs right now
  const inflightRes = await client.query(`
    SELECT (payload->>'workId') as work_id, count(*) as cnt
    FROM importer_queue
    WHERE status = 'IMPORTING'
    GROUP BY (payload->>'workId')
  `);
  console.log(`=== IN-FLIGHT WORKS COUNT: ${inflightRes.rows.length} ===`);

  // 4. In active works, how many QUEUED jobs exist?
  if (activeWorkIds.length > 0) {
    const activeQueuedRes = await client.query(`
      SELECT (payload->>'workId') as work_id, count(*) as queued_cnt
      FROM importer_queue
      WHERE status = 'QUEUED' AND (payload->>'workId') = ANY($1::text[])
      GROUP BY (payload->>'workId')
    `, [activeWorkIds]);
    console.log(`Active works with queued jobs: ${activeQueuedRes.rows.length}`);
    let totalActiveQueued = 0;
    for (const r of activeQueuedRes.rows) totalActiveQueued += parseInt(r.queued_cnt, 10);
    console.log(`Total queued jobs in active works: ${totalActiveQueued}`);
  }

  // 5. Total QUEUED jobs across ALL works (claimable vs unadmitted)
  const totalQueuedWorks = await client.query(`
    SELECT count(DISTINCT (payload->>'workId')) as distinct_works, count(*) as total_queued
    FROM importer_queue
    WHERE status = 'QUEUED'
  `);
  console.log("Total QUEUED jobs and distinct works:", totalQueuedWorks.rows[0]);

  // 6. Source distribution of queued jobs
  const sourceQueuedRes = await client.query(`
    SELECT source, count(*) as count
    FROM importer_queue
    WHERE status = 'QUEUED'
    GROUP BY source
    ORDER BY count DESC
    LIMIT 10
  `);
  console.log("=== TOP 10 SOURCES IN QUEUE ===");
  console.table(sourceQueuedRes.rows);

  await client.end();
}
run().catch(console.error);
