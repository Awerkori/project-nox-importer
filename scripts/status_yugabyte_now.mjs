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
  ssl: { rejectUnauthorized: false }
});

async function main() {
  await client.connect();

  // 1. max_connections
  const maxConnRes = await client.query('SHOW max_connections');
  const maxConnections = parseInt(maxConnRes.rows[0].max_connections, 10);

  // 2. pg_stat_activity breakdown
  const actRes = await client.query(`
    SELECT pid, datname, usename, application_name, client_addr, backend_type, state, 
           state_change, query_start, backend_start, wait_event_type, wait_event, query
    FROM pg_stat_activity
    ORDER BY backend_start ASC
  `);

  // 3. Yugabyte CPU metrics
  let ybMetrics = null;
  try {
    const ybMetricsRes = await client.query('SELECT * FROM yb_servers_metrics LIMIT 1');
    ybMetrics = ybMetricsRes.rows[0];
  } catch (e) {
    console.error('yb_servers_metrics error:', e.message);
  }

  // 4. Settings (publication barrier)
  const settingsRes = await client.query('SELECT * FROM settings');

  // 5. Queue status
  const queueRes = await client.query(`
    SELECT status, count(*), 
           count(*) FILTER (WHERE task_type = 'IMPORT_CHAPTER') as chapters
    FROM importer_queue
    GROUP BY status
  `);

  // 6. Active jobs details
  const activeJobsRes = await client.query(`
    SELECT id, task_type, source, status, locked_by, locked_at, lease_expires_at, 
           progress_current, progress_total, updated_at, created_at
    FROM importer_queue
    WHERE status = 'IMPORTING'
    ORDER BY updated_at DESC
  `);

  // 7. Importer telemetry table
  let recentTelem = [];
  try {
    const telemRes = await client.query(`
      SELECT *
      FROM importer_telemetry
      ORDER BY created_at DESC
      LIMIT 30
    `);
    recentTelem = telemRes.rows;
  } catch (e) {
    console.error('importer_telemetry error:', e.message);
  }

  // 8. Recent completed chapters in last 5m, 10m, 15m
  const comp5m = await client.query(`
    SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
    FROM importer_queue
    WHERE status = 'COMPLETED' 
      AND task_type = 'IMPORT_CHAPTER'
      AND updated_at >= NOW() - INTERVAL '5 minutes'
  `);
  const comp10m = await client.query(`
    SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
    FROM importer_queue
    WHERE status = 'COMPLETED' 
      AND task_type = 'IMPORT_CHAPTER'
      AND updated_at >= NOW() - INTERVAL '10 minutes'
  `);
  const comp15m = await client.query(`
    SELECT count(*) as count, COALESCE(sum(progress_current), 0) as pages
    FROM importer_queue
    WHERE status = 'COMPLETED' 
      AND task_type = 'IMPORT_CHAPTER'
      AND updated_at >= NOW() - INTERVAL '15 minutes'
  `);

  // 9. Recent media in last 5m, 10m, 15m
  const med5m = await client.query(`
    SELECT count(*) as count, COALESCE(sum(bytes), 0) as bytes
    FROM media
    WHERE created_at >= NOW() - INTERVAL '5 minutes'
  `);
  const med10m = await client.query(`
    SELECT count(*) as count, COALESCE(sum(bytes), 0) as bytes
    FROM media
    WHERE created_at >= NOW() - INTERVAL '10 minutes'
  `);

  // 10. Ungranted locks
  const locksRes = await client.query('SELECT count(*) FROM pg_locks WHERE NOT granted');

  // 11. Recent errors / retries / failures in importer_queue
  const recentErrors = await client.query(`
    SELECT id, task_type, source, status, last_error, attempts, updated_at
    FROM importer_queue
    WHERE (status IN ('FAILED', 'RETRY') OR last_error IS NOT NULL)
      AND updated_at >= NOW() - INTERVAL '30 minutes'
    ORDER BY updated_at DESC
    LIMIT 10
  `);

  console.log(JSON.stringify({
    maxConnections,
    activity: actRes.rows,
    ybMetrics,
    settings: settingsRes.rows,
    queue: queueRes.rows,
    activeJobs: activeJobsRes.rows,
    recentTelem,
    comp5m: comp5m.rows[0],
    comp10m: comp10m.rows[0],
    comp15m: comp15m.rows[0],
    med5m: med5m.rows[0],
    med10m: med10m.rows[0],
    ungrantedLocks: locksRes.rows[0].count,
    recentErrors: recentErrors.rows
  }, null, 2));

  await client.end();
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
