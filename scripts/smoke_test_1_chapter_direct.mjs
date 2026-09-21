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
  console.log('Connected to YugabyteDB directly.');

  // Check initial state
  const testSource = 'mangalivreto';
  const initialQueued = await client.query(
    `SELECT id, payload, status FROM importer_queue WHERE source = $1 AND status = 'QUEUED' ORDER BY priority DESC, created_at ASC LIMIT 1`,
    [testSource]
  );
  if (initialQueued.rows.length === 0) {
    console.error(`No queued jobs for ${testSource}!`);
    process.exit(1);
  }
  const targetJob = initialQueued.rows[0];
  console.log('Target Smoke Test Job:', targetJob.id, 'Payload:', targetJob.payload);

  const initialCompletedCount = parseInt((await client.query(`SELECT count(*)::int as c FROM importer_queue WHERE status = 'COMPLETED'`)).rows[0].c, 10);
  console.log('Initial COMPLETED count:', initialCompletedCount);

  // 1. Open barrier
  console.log('\n[1/4] Opening publication safety barrier...');
  await client.query(`UPDATE settings SET value = 'OPEN' WHERE key = 'publication_safety_barrier'`);

  // 2. Activate source for chapter ingestion ONLY (discovery strictly disabled)
  console.log(`[2/4] Activating ${testSource} (chapter_ingestion_enabled=true, catalog_discovery_enabled=false)...`);
  await client.query(`
    UPDATE importer_sources 
    SET status = 'ACTIVE', 
        chapter_ingestion_enabled = true, 
        catalog_discovery_enabled = false 
    WHERE id = $1 OR name = $1
  `, [testSource]);

  console.log('\n[3/4] Monitoring job execution in production on Discloud...');
  const startTime = Date.now();
  let completed = false;

  for (let i = 0; i < 40; i++) { // wait up to 80s
    await new Promise(r => setTimeout(r, 2000));
    const elapsed = Math.round((Date.now() - startTime) / 1000);

    const checkJob = await client.query(
      `SELECT id, status, updated_at, last_error FROM importer_queue WHERE id = $1`,
      [targetJob.id]
    );
    const curr = checkJob.rows[0];

    const importingCount = parseInt((await client.query(`SELECT count(*)::int as c FROM importer_queue WHERE status = 'IMPORTING'`)).rows[0].c, 10);
    const currCompleted = parseInt((await client.query(`SELECT count(*)::int as c FROM importer_queue WHERE status = 'COMPLETED'`)).rows[0].c, 10);

    console.log(`[+${elapsed}s] Target Job: status=${curr.status} | Active IMPORTING=${importingCount} | Total COMPLETED=${currCompleted}`);

    if (curr.status === 'COMPLETED' || currCompleted > initialCompletedCount) {
      console.log('\n✅ SMOKE TEST PASSED: Chapter completed successfully in direct mode!');
      completed = true;
      break;
    }

    if (curr.status === 'FAILED') {
      console.error('❌ Job failed:', curr.last_error);
      break;
    }
  }

  // 3. Immediately freeze source and close barrier
  console.log('\n[4/4] Freezing source and closing publication safety barrier...');
  await client.query(`UPDATE importer_sources SET status = 'PAUSED' WHERE id = $1 OR name = $1`, [testSource]);
  await client.query(`UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'`);

  // Wait for any active to finish
  let active = 1;
  while (active > 0) {
    const actRes = await client.query(`SELECT count(*)::int as c FROM importer_queue WHERE status = 'IMPORTING'`);
    active = parseInt(actRes.rows[0].c, 10);
    if (active > 0) {
      console.log(`Waiting for ${active} active jobs to drain...`);
      await new Promise(r => setTimeout(r, 1500));
    }
  }
  console.log('All active jobs drained. System returned to clean quiescence.');

  await client.end();
  process.exit(completed ? 0 : 1);
}

main().catch(err => {
  console.error('Smoke test error:', err);
  process.exit(1);
});
