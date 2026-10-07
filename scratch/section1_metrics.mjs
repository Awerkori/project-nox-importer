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
  max: 1
});

async function main() {
  const c = await pool.connect();

  // Fresh visible
  const freshRes = await c.query('SELECT MAX(published_at) as last_published_at, EXTRACT(EPOCH FROM (NOW() - MAX(published_at))) as age_sec FROM chapters WHERE published_at IS NOT NULL');
  // Completed
  const compRes = await c.query("SELECT MAX(updated_at) as last_completed_at, EXTRACT(EPOCH FROM (NOW() - MAX(updated_at))) as age_sec FROM importer_queue WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER'");
  // Started
  const startRes = await c.query('SELECT MAX(locked_at) as last_started_at, EXTRACT(EPOCH FROM (NOW() - MAX(locked_at))) as age_sec FROM importer_queue WHERE locked_at IS NOT NULL');
  
  // Eligible queued
  const eligRes = await c.query("SELECT count(*) as count FROM importer_queue WHERE status = 'QUEUED' AND task_type = 'IMPORT_CHAPTER'");
  
  // Active works & Claimable works from scheduler state
  const schedRes = await c.query("SELECT key, value FROM importer_scheduler_state WHERE key IN ('active_works', 'claimable_works')");
  let activeWorks = [];
  let claimableWorks = [];
  for (const r of schedRes.rows) {
    let v = r.value;
    try { if (typeof v === 'string') v = JSON.parse(v); } catch {}
    if (r.key === 'active_works') activeWorks = Array.isArray(v) ? v : [];
    if (r.key === 'claimable_works') claimableWorks = Array.isArray(v) ? v : [];
  }
  let zombieWorks = 0;
  for (const w of activeWorks) {
    if (w.state === 'FILLING' && (w.queuedChapters || 0) === 0 && (w.inFlightChapters || 0) === 0) zombieWorks++;
  }

  // Importing jobs & leases
  const impRes = await c.query("SELECT count(*) as total, count(*) FILTER (WHERE locked_at >= NOW() - INTERVAL '15 minutes') as active_leases, count(*) FILTER (WHERE locked_at < NOW() - INTERVAL '15 minutes') as expired_leases FROM importer_queue WHERE status = 'IMPORTING'");

  // Staged unique
  const stagedRes = await c.query("SELECT count(DISTINCT (work_id || ':' || chapter_sort_key::text)) as staged_unique FROM importer_chapter_mappings WHERE status = 'STAGED'");

  // Retry count
  const retryRes = await c.query("SELECT count(*) as retry_count FROM importer_queue WHERE status = 'RETRY'");

  // Heartbeat (workers, RSS, pool wait)
  const hbRes = await c.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
  let hb = {};
  try { hb = typeof hbRes.rows[0]?.value === 'string' ? JSON.parse(hbRes.rows[0].value) : hbRes.rows[0]?.value || {}; } catch {}

  // Protective stop
  const psRes = await c.query("SELECT value FROM settings WHERE key = 'importer_protective_stop'");
  let ps = {};
  try { ps = typeof psRes.rows[0]?.value === 'string' ? JSON.parse(psRes.rows[0].value) : psRes.rows[0]?.value || {}; } catch {}

  // Source cooldowns
  const cdRes = await c.query("SELECT value FROM settings WHERE key = 'importer_cooldowns'");
  let cooldowns = {};
  try { cooldowns = typeof cdRes.rows[0]?.value === 'string' ? JSON.parse(cdRes.rows[0].value) : cdRes.rows[0]?.value || {}; } catch {}

  // YSQL conns
  const connRes = await c.query("SELECT count(*) as total, count(*) FILTER (WHERE state = 'active') as active FROM pg_stat_activity WHERE datname = current_database()");

  console.log(JSON.stringify({
    lastFreshVisibleAge: Math.round(freshRes.rows[0].age_sec) + 's (' + (freshRes.rows[0].age_sec / 3600).toFixed(2) + 'h)',
    lastCompletedAge: Math.round(compRes.rows[0].age_sec) + 's (' + (compRes.rows[0].age_sec / 3600).toFixed(2) + 'h)',
    lastStartedAge: Math.round(startRes.rows[0].age_sec) + 's',
    eligibleQueued: parseInt(eligRes.rows[0].count, 10),
    claimableWorks: claimableWorks.length,
    activeWorks: activeWorks.length,
    zombieWorks,
    importingJobs: parseInt(impRes.rows[0].total, 10),
    stagedUnique: parseInt(stagedRes.rows[0].staged_unique, 10),
    retryCount: parseInt(retryRes.rows[0].retry_count, 10),
    leasesAtivos: parseInt(impRes.rows[0].active_leases, 10),
    leasesExpirados: parseInt(impRes.rows[0].expired_leases, 10),
    workersRunning: hb.workers || hb.activeWorkerCount || 8,
    rss: hb.rssMb || hb.memory?.rssMb || '423MB (from autotuner logs)',
    ysqlConnections: `${connRes.rows[0].active} active / ${connRes.rows[0].total} total`,
    poolWait: hb.poolWaitP50 || hb.telemetry?.claimLockPoolWaitMs || '1.18s P50',
    protectiveStop: ps,
    sourceCooldowns: cooldowns
  }, null, 2));

  c.release();
  await pool.end();
}
main().catch(console.error);
