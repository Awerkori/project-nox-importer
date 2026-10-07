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

  console.log('=== VERIFY LIVE CLAIMS & ADMISSIONS POST-DEPLOY ===');

  // 1. P1 Claimable Count right now
  const p1Res = await client.query(`
    SELECT 
      COUNT(CASE WHEN q.status IN ('QUEUED', 'RETRY') AND (q.next_run_at IS NULL OR q.next_run_at <= NOW()) THEN 1 END) as claimable_cnt,
      COUNT(CASE WHEN q.status = 'PAUSED_BY_STAFF' THEN 1 END) as paused_cnt,
      COUNT(DISTINCT q.payload->>'workId') as works_cnt
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    JOIN importer_sources s ON s.id = q.source
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
      AND w.published = true
      AND s.enabled = true
      AND s.status = 'ACTIVE'
      AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW());
  `);
  console.log('P1 Status:');
  console.table(p1Res.rows);

  // 2. Jobs locked / claimed in the last 5 minutes by work lane & source
  const claimsRes = await client.query(`
    SELECT 
      w.published as is_catalog_work,
      q.source,
      COUNT(*) as claimed_count
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND q.locked_at >= NOW() - INTERVAL '5 minutes'
    GROUP BY w.published, q.source
    ORDER BY claimed_count DESC;
  `);
  console.log('Claims in Last 5 Minutes:');
  console.table(claimsRes.rows);

  // 3. Any new works created in works table in the last 15 minutes
  const newWorksRes = await client.query(`
    SELECT id, title, created_at
    FROM works
    WHERE created_at >= NOW() - INTERVAL '15 minutes'
  `);
  console.log(`New Works Created in 'works' (Last 15m): ${newWorksRes.rows.length}`);
  if (newWorksRes.rows.length > 0) {
    console.table(newWorksRes.rows);
  }

  // 4. Any works admitted in importer_work_mappings in the last 15 minutes
  const newMappingsRes = await client.query(`
    SELECT work_id, source_title, source, sync_status, created_at
    FROM importer_work_mappings
    WHERE created_at >= NOW() - INTERVAL '15 minutes'
  `);
  console.log(`New Work Mappings Created (Last 15m): ${newMappingsRes.rows.length}`);
  if (newMappingsRes.rows.length > 0) {
    console.table(newMappingsRes.rows);
  }

  // 5. Check if any WAITING_ADMISSION works exist
  const waitingRes = await client.query(`
    SELECT count(*) as waiting_count
    FROM importer_work_mappings
    WHERE sync_status = 'WAITING_ADMISSION'
  `);
  console.log('Works currently held in WAITING_ADMISSION:', waitingRes.rows[0].waiting_count);

  await client.end();
}

main().catch(console.error);
