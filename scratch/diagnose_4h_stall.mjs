import pg from 'pg';
import fs from 'fs';
import dotenv from 'dotenv';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: true, ca: fs.readFileSync('./config/root.crt').toString() },
  max: 2,
  connectionTimeoutMillis: 10000,
  statement_timeout: 30000
});

async function main() {
  const client = await pool.connect();
  const now = new Date();
  console.log('=== DIAGNÓSTICO DO INCIDENTE DE STALL (>4 HORAS) ===');
  console.log('Current Timestamp:', now.toISOString());

  // 1. SETTINGS & HEARTBEAT & PROTECTIVE STOP
  const settingsRes = await client.query("SELECT key, value FROM settings WHERE key IN ('importer_heartbeat', 'importer_status', 'importer_state', 'importer_protective_stop', 'publication_safety_barrier', 'importer_metrics', 'importer_cooldowns', 'active_works')");
  console.log('\n--- SETTINGS & HEARTBEAT ---');
  for (const row of settingsRes.rows) {
    let val = row.value;
    try {
      if (typeof val === 'string') val = JSON.parse(val);
    } catch {}
    console.log(`[${row.key}]:`, typeof val === 'object' ? JSON.stringify(val).slice(0, 300) : val);
  }

  // 2. SCHEDULER STATE (importer_scheduler_state)
  const schedRes = await client.query("SELECT key, value, updated_at FROM importer_scheduler_state");
  console.log('\n--- SCHEDULER STATE ---');
  let activeWorks = [];
  for (const row of schedRes.rows) {
    let val = row.value;
    try {
      if (typeof val === 'string') val = JSON.parse(val);
    } catch {}
    console.log(`[${row.key}] (updated_at: ${row.updated_at}):`, typeof val === 'object' ? (Array.isArray(val) ? `Array(${val.length})` : Object.keys(val)) : val);
    if (row.key === 'active_works') {
      activeWorks = Array.isArray(val) ? val : [];
    }
  }

  // Active works detail
  let zombieWorks = 0;
  let activeWorksWithInflight = 0;
  for (const w of activeWorks) {
    if (w.state === 'FILLING' && (w.queuedChapters || 0) === 0 && (w.inFlightChapters || 0) === 0) {
      zombieWorks++;
    }
    if ((w.inFlightChapters || 0) > 0) activeWorksWithInflight++;
  }
  console.log(`Active Works: ${activeWorks.length} | Zombie Works: ${zombieWorks} | With InFlight: ${activeWorksWithInflight}`);
  if (activeWorks.length > 0) {
    console.log('Active Works Sample (up to 5):', JSON.stringify(activeWorks.slice(0, 5), null, 2));
  }

  // 3. QUEUE STATUS & JOBS
  console.log('\n--- QUEUE & LEASES ---');
  const qStats = await client.query(`
    SELECT status, count(*) as count
    FROM importer_queue
    GROUP BY status
    ORDER BY count DESC
  `);
  console.log('Queue by Status:', qStats.rows);

  // Eligible queued jobs
  const eligRes = await client.query(`
    SELECT count(*) as eligible_queued
    FROM importer_queue
    WHERE status = 'QUEUED' AND task_type = 'IMPORT_CHAPTER'
  `);
  console.log('Eligible Queued Chapters:', eligRes.rows[0].eligible_queued);

  // Retry jobs
  const retryRes = await client.query(`
    SELECT count(*) as retry_count
    FROM importer_queue
    WHERE status = 'RETRY'
  `);
  console.log('Retry Count:', retryRes.rows[0].retry_count);

  // Importing jobs & leases
  const impJobs = await client.query(`
    SELECT id, task_type, source, chapter_sort_key, attempts, locked_by, locked_at, updated_at,
           EXTRACT(EPOCH FROM (NOW() - locked_at)) as locked_sec_ago,
           EXTRACT(EPOCH FROM (NOW() - updated_at)) as updated_sec_ago
    FROM importer_queue
    WHERE status = 'IMPORTING'
    ORDER BY locked_at ASC
  `);
  console.log(`IMPORTING jobs: ${impJobs.rows.length}`);
  let activeLeases = 0;
  let expiredLeases = 0;
  for (const j of impJobs.rows) {
    // Default lease is usually 10-15 minutes (600-900s)
    if (j.locked_sec_ago > 900) {
      expiredLeases++;
    } else {
      activeLeases++;
    }
  }
  console.log(`Active Leases (<=900s): ${activeLeases} | Expired Leases (>900s): ${expiredLeases}`);
  if (impJobs.rows.length > 0) {
    console.log('IMPORTING jobs detail:', impJobs.rows.slice(0, 10));
  }

  // 4. TIMESTAMPS: LAST STARTED, LAST COMPLETED, LAST FRESH VISIBLE
  console.log('\n--- TIMESTAMPS & PROGRESS AGES ---');
  // Last started
  const lastStartedRes = await client.query(`
    SELECT MAX(locked_at) as last_started_at,
           EXTRACT(EPOCH FROM (NOW() - MAX(locked_at))) as started_sec_ago
    FROM importer_queue
    WHERE locked_at IS NOT NULL
  `);
  console.log('Last Started:', lastStartedRes.rows[0]);

  // Last completed
  const lastCompRes = await client.query(`
    SELECT MAX(updated_at) as last_completed_at,
           EXTRACT(EPOCH FROM (NOW() - MAX(updated_at))) as completed_sec_ago
    FROM importer_queue
    WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER'
  `);
  console.log('Last Completed Job:', lastCompRes.rows[0]);

  // Last fresh visible chapter (published_at)
  const lastPubRes = await client.query(`
    SELECT MAX(published_at) as last_published_at,
           EXTRACT(EPOCH FROM (NOW() - MAX(published_at))) as published_sec_ago
    FROM chapters
    WHERE published_at IS NOT NULL
  `);
  console.log('Last Published Chapter:', lastPubRes.rows[0]);

  // Last 15 min stats
  const stats15m = await client.query(`
    SELECT 
      (SELECT count(*) FROM importer_queue WHERE locked_at >= NOW() - INTERVAL '15 minutes') as started_15m,
      (SELECT count(*) FROM importer_queue WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' AND updated_at >= NOW() - INTERVAL '15 minutes') as completed_15m,
      (SELECT count(*) FROM chapters WHERE published_at >= NOW() - INTERVAL '15 minutes') as fresh_15m
  `);
  console.log('Last 15 minutes progress:', stats15m.rows[0]);

  // 5. STAGED UNIQUE & MAPPINGS
  console.log('\n--- MAPPINGS & STAGED ---');
  const stagedRes = await client.query(`
    SELECT count(DISTINCT (work_id || ':' || chapter_sort_key::text)) as staged_unique,
           count(*) as staged_total
    FROM importer_chapter_mappings
    WHERE status = 'STAGED'
  `);
  console.log('Staged Mappings:', stagedRes.rows[0]);

  // 6. DB CONNECTIONS & POOL HEALTH
  console.log('\n--- YSQL CONNECTIONS ---');
  const conns = await client.query(`
    SELECT count(*) as total_conns,
           count(*) FILTER (WHERE state = 'active') as active_conns,
           count(*) FILTER (WHERE state = 'idle') as idle_conns,
           count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
    FROM pg_stat_activity
    WHERE datname = current_database()
  `);
  console.log('YSQL Connections:', conns.rows[0]);

  client.release();
  await pool.end();
}

main().catch(err => {
  console.error('Diagnostic error:', err);
  process.exit(1);
});
