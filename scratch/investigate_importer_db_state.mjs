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
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 10000
});

async function main() {
  await client.connect();
  console.log('Connected to YugabyteDB production');
  try {
    console.log('\n=== 1. SETTINGS & PROTECTIONS ===');
    const settingsRes = await client.query(`
      SELECT * FROM settings 
      WHERE key IN ('importer_protective_stop', 'publication_safety_barrier', 'work_affinity_scheduler_enabled', 'catalog_discovery_enabled', 'protective_stop_history')
    `);
    console.table(settingsRes.rows);

    console.log('\n=== 2. YSQL CONNECTIONS & ACTIVE QUERIES ===');
    const connRes = await client.query(`
      SELECT state, count(*) as count 
      FROM pg_stat_activity 
      WHERE datname = current_database() 
      GROUP BY state
    `);
    console.table(connRes.rows);

    const activeQueries = await client.query(`
      SELECT pid, application_name, client_addr, state, now() - query_start as duration, query
      FROM pg_stat_activity
      WHERE datname = current_database() AND state != 'idle'
      ORDER BY duration DESC
      LIMIT 10
    `);
    console.log('Active queries count:', activeQueries.rowCount);
    if (activeQueries.rowCount > 0) {
      console.table(activeQueries.rows.map(r => ({
        pid: r.pid,
        app: r.application_name,
        state: r.state,
        duration: r.duration,
        query: (r.query || '').substring(0, 80)
      })));
    }

    console.log('\n=== 3. CHAPTER QUEUE STATUS BREAKDOWN ===');
    const queueStatusRes = await client.query(`
      SELECT status, priority, count(*) 
      FROM importer_queue 
      GROUP BY status, priority 
      ORDER BY status, priority DESC
    `);
    console.table(queueStatusRes.rows);

    console.log('\n=== 4. CLAIMABLE JOBS RIGHT NOW ===');
    const claimableRes = await client.query(`
      SELECT priority, count(*)
      FROM importer_queue
      WHERE status = 'QUEUED'
      GROUP BY priority
      ORDER BY priority DESC
    `);
    console.table(claimableRes.rows);

    console.log('\n=== 5. STALE LEASES (IMPORTING > 5m) ===');
    const staleRes = await client.query(`
      SELECT id, payload->>'work_id' as work_id, source, priority, locked_at, locked_by, now() - locked_at as locked_age
      FROM importer_queue
      WHERE status = 'IMPORTING' AND locked_at < NOW() - INTERVAL '5 minutes'
      ORDER BY locked_at ASC
      LIMIT 10
    `);
    console.log('Stale leases count:', staleRes.rowCount);
    if (staleRes.rowCount > 0) {
      console.table(staleRes.rows);
    }

    console.log('\n=== 6. IMPORTING JOBS RIGHT NOW ===');
    const importingRes = await client.query(`
      SELECT id, payload->>'work_id' as work_id, source, priority, locked_at, locked_by, now() - locked_at as age
      FROM importer_queue
      WHERE status = 'IMPORTING'
      ORDER BY locked_at DESC
      LIMIT 10
    `);
    console.log('Total IMPORTING jobs:', importingRes.rowCount);
    if (importingRes.rowCount > 0) {
      console.table(importingRes.rows);
    }

    console.log('\n=== 7. RECENT JOB ACTIVITY TIMESTAMPS ===');
    const lastClaim = await client.query(`
      SELECT id, payload->>'work_id' as work_id, source, priority, locked_at, now() - locked_at as age
      FROM importer_queue
      WHERE locked_at IS NOT NULL
      ORDER BY locked_at DESC
      LIMIT 1
    `);
    console.log('Last Claim:', lastClaim.rows[0] || 'None');

    const lastCompletion = await client.query(`
      SELECT id, payload->>'work_id' as work_id, source, priority, updated_at, now() - updated_at as age
      FROM importer_queue
      WHERE status = 'COMPLETED'
      ORDER BY updated_at DESC
      LIMIT 1
    `);
    console.log('Last Completion:', lastCompletion.rows[0] || 'None');

    console.log('\n=== 8. RECENT CHAPTER PUBLICATIONS TIMESTAMPS ===');
    const lastPublication = await client.query(`
      SELECT id, work_id, number, published_at, now() - published_at as age
      FROM chapters
      WHERE published_at IS NOT NULL
      ORDER BY published_at DESC
      LIMIT 5
    `);
    console.table(lastPublication.rows);

    console.log('\n=== 9. RECENT CHAPTERS PUBLISHED (LAST 6 HOURS) ===');
    const recentCount = await client.query(`
      SELECT count(*) as count, max(published_at) as latest_pub, now() - max(published_at) as time_since_last_pub
      FROM chapters
      WHERE published_at > NOW() - INTERVAL '6 hours'
    `);
    console.table(recentCount.rows);

    console.log('\n=== 10. ACTIVE VS BLOCKED SOURCES ===');
    const sourcesRes = await client.query(`
      SELECT id, name, status, enabled, cooldown_until, now() < cooldown_until as in_cooldown, blocked_reason
      FROM importer_sources
      ORDER BY id
    `);
    console.table(sourcesRes.rows);

    console.log('\n=== 11. WORK MAPPINGS IN WAITING_ADMISSION ===');
    const waitingRes = await client.query(`
      SELECT status, count(*) 
      FROM importer_work_mappings
      GROUP BY status
    `);
    console.table(waitingRes.rows);

    console.log('\n=== 12. DUPLICATE WORKS AUDIT ===');
    const dupes = await client.query(`
      SELECT lower(trim(title)) as norm_title, count(*) as count
      FROM works
      GROUP BY lower(trim(title))
      HAVING count(*) > 1
    `);
    console.log('Duplicate works count:', dupes.rowCount);
    if (dupes.rowCount > 0) {
      console.table(dupes.rows);
    }

  } finally {
    await client.end();
  }
}

main().catch(err => {
  console.error('Fatal DB investigation error:', err);
  process.exit(1);
});
