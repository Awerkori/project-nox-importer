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

  console.log('======================================================================');
  console.log('🧪 STEP 1: CONFIGURING DECOUPLED DISCOVERY & CHAPTER INGESTION');
  console.log('======================================================================\n');

  // 1. Ensure global setting catalog_discovery_enabled is DISABLED
  await client.query(`
    INSERT INTO settings (key, value)
    VALUES ('catalog_discovery_enabled', 'DISABLED')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
  `);

  // 2. Disable catalog discovery for all sources
  await client.query(`
    UPDATE importer_sources
    SET catalog_discovery_enabled = false;
  `);

  // 3. Enable chapter ingestion and activate operational chapter sources
  const actRes = await client.query(`
    UPDATE importer_sources
    SET chapter_ingestion_enabled = true,
        catalog_discovery_enabled = false,
        status = 'ACTIVE',
        updated_at = NOW()
    WHERE id IN ('hanamiheaven', 'fleurblanche', 'mangalivreto')
    RETURNING id, status, chapter_ingestion_enabled, catalog_discovery_enabled
  `);
  console.log('Active Chapter Sources Configured:');
  console.table(actRes.rows);

  // 4. Record baseline queue counts at T=0
  const t0Res = await client.query('SELECT NOW() as db_time');
  const t0 = t0Res.rows[0].db_time;
  console.log(`\nBaseline DB Timestamp: ${t0.toISOString()}`);

  const initRes = await client.query(`
    SELECT task_type, status, count(*) 
    FROM importer_queue
    WHERE created_at >= $1
    GROUP BY task_type, status
  `, [t0]);
  console.log(`Initial jobs created since T0: ${initRes.rowCount}`);

  // Keep publication safety barrier CLOSED during the scheduler cycle test
  await client.query("UPDATE settings SET value = 'CLOSED' WHERE key = 'publication_safety_barrier'");

  console.log('\n[Wait] Waiting 45 seconds (exceeding 30s scheduler cycle) to monitor if any DISCOVER_WORKS or SYNC_WORK is spawned...');
  await new Promise(r => setTimeout(r, 45000));

  // 5. Verification after scheduler cycle
  console.log('\n======================================================================');
  console.log('🧪 STEP 2: VERIFYING SCHEDULER CYCLE (DISCOVERY MUST REMAIN ZERO)');
  console.log('======================================================================\n');

  const afterRes = await client.query(`
    SELECT task_type, status, count(*) 
    FROM importer_queue
    WHERE created_at >= $1
    GROUP BY task_type, status
  `, [t0]);

  const discoverCreated = afterRes.rows.filter(r => r.task_type === 'DISCOVER_WORKS').reduce((a, b) => a + parseInt(b.count, 10), 0);
  const syncCreated = afterRes.rows.filter(r => r.task_type === 'SYNC_WORK').reduce((a, b) => a + parseInt(b.count, 10), 0);

  console.log(`DISCOVER_WORKS created during cycle: ${discoverCreated}`);
  console.log(`SYNC_WORK created during cycle: ${syncCreated}`);

  // 6. Confirm chapter eligibility
  const chapRes = await client.query(`
    SELECT count(*) as queued_chapters
    FROM importer_queue
    WHERE task_type = 'IMPORT_CHAPTER' 
      AND status = 'QUEUED'
      AND source IN ('hanamiheaven', 'fleurblanche', 'mangalivreto')
      AND next_run_at <= NOW()
  `);
  const queuedChapters = parseInt(chapRes.rows[0].queued_chapters, 10);
  console.log(`IMPORT_CHAPTER eligible in queue: ${queuedChapters}`);

  const passed = discoverCreated === 0 && syncCreated === 0 && queuedChapters > 0;

  console.log('\n======================================================================');
  console.log(`VALIDATION RESULT: ${passed ? '✅ PASSED' : '❌ FAILED'}`);
  console.log('======================================================================');
  console.log(`DISCOVER_WORKS created = ${discoverCreated}`);
  console.log(`SYNC_WORK created by discovery = ${syncCreated}`);
  console.log(`IMPORT_CHAPTER continua elegível = ${queuedChapters > 0 ? 'YES' : 'NO'} (${queuedChapters} jobs)`);

  await client.end();
  process.exit(passed ? 0 : 1);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
