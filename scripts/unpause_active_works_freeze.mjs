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

  const stateRes = await client.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
  const activeWorksMap = stateRes.rows[0]?.value || {};
  const activeWorks = Array.isArray(activeWorksMap) ? activeWorksMap : Object.values(activeWorksMap);
  const activeWorkIds = activeWorks.map(w => w.workId);

  console.log(`Active works count: ${activeWorkIds.length}`);

  // Unpause EMERGENCY_FREEZE_MIGRATION only for admitted active works
  const unpauseRes = await client.query(`
    UPDATE importer_queue
    SET status = 'QUEUED',
        pause_reason = null,
        next_run_at = NOW(),
        updated_at = NOW()
    WHERE status = 'PAUSED_BY_STAFF'
      AND pause_reason = 'EMERGENCY_FREEZE_MIGRATION'
      AND (payload->>'workId') = ANY($1::text[])
    RETURNING id;
  `, [activeWorkIds]);

  console.log(`Successfully unpaused ${unpauseRes.rows.length} jobs for active works!`);

  // Also clean up any unpaused jobs that are already published
  const cleanRes = await client.query(`
    UPDATE importer_queue q
    SET status = 'COMPLETED',
        last_error = 'CANONICAL_ALREADY_SATISFIED',
        updated_at = NOW()
    FROM chapters c
    WHERE q.status = 'QUEUED'
      AND (q.payload->>'workId')::uuid = c.work_id
      AND (q.chapter_sort_key = c.number OR (q.payload->>'chapterNumber')::numeric = c.number)
      AND c.published_at IS NOT NULL
      AND (q.payload->>'workId') = ANY($1::text[])
    RETURNING q.id;
  `, [activeWorkIds]);

  console.log(`Auto-satisfied ${cleanRes.rows.length} unpaused jobs that were already published!`);

  await client.end();
}
run().catch(console.error);
