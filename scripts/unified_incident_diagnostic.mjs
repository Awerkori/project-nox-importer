import pg from 'pg';
import fs from 'fs';

const envVars = Object.fromEntries(
  fs.readFileSync('/home/awerkori/.config/project-nox/yugabyte.env', 'utf8')
    .split('\n')
    .filter(l => l.includes('=') && !l.startsWith('#'))
    .map(l => [l.split('=')[0].trim(), l.substring(l.indexOf('=') + 1).trim().replace(/^['"]|['"]$/g, '')])
);

const pool = new pg.Pool({
  host: envVars.YUGABYTE_HOST,
  port: parseInt(envVars.YUGABYTE_PORT || '5433', 10),
  user: envVars.YUGABYTE_USER,
  password: envVars.YUGABYTE_PASSWORD,
  database: envVars.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 1,
  connectionTimeoutMillis: 10000
});

async function main() {
  const client = await pool.connect();
  const report = {};

  try {
    // 1. Table sizes & row counts
    const tableCounts = await client.query(`
      SELECT 
        (SELECT count(*) FROM importer_queue) as queue_total,
        (SELECT count(*) FROM importer_chapter_mappings) as mappings_total,
        (SELECT count(*) FROM chapters) as chapters_total,
        (SELECT count(*) FROM works) as works_total,
        (SELECT count(*) FROM session) as session_total;
    `);
    report.tableCounts = tableCounts.rows[0];

    // 2. Queue breakdown
    const qBreakdown = await client.query(`
      SELECT status, task_type, count(*) as cnt
      FROM importer_queue
      GROUP BY status, task_type
      ORDER BY count(*) DESC;
    `);
    report.queueBreakdown = qBreakdown.rows;

    // 3. Staged breakdown
    const stagedBreakdown = await client.query(`
      SELECT count(*) as total_staged,
             count(DISTINCT work_id) as staged_works,
             MIN(created_at) as oldest_staged,
             MAX(created_at) as newest_staged
      FROM importer_chapter_mappings
      WHERE status = 'STAGED';
    `);
    report.stagedBreakdown = stagedBreakdown.rows[0];

    // 4. pg_stat_statements top 10 by total_time
    const topQueries = await client.query(`
      SELECT query, calls, round(total_exec_time::numeric, 2) as total_ms,
             round(mean_exec_time::numeric, 2) as mean_ms, rows
      FROM pg_stat_statements
      ORDER BY total_exec_time DESC
      LIMIT 10;
    `);
    report.topQueries = topQueries.rows;

    // 5. pg_stat_activity right now
    const activity = await client.query(`
      SELECT pid, state, wait_event_type, wait_event,
             round(EXTRACT(EPOCH FROM (now() - query_start))::numeric, 2) as duration_sec,
             substring(query, 1, 180) as query_snip
      FROM pg_stat_activity
      WHERE datname = current_database() AND state != 'idle' AND pid != pg_backend_pid();
    `);
    report.activity = activity.rows;

    // 6. Recent publication rate in 5m and 15m
    const pubs = await client.query(`
      SELECT 
        COUNT(CASE WHEN published_at >= NOW() - INTERVAL '5 minutes' THEN 1 END) as pub_5m,
        COUNT(CASE WHEN published_at >= NOW() - INTERVAL '15 minutes' THEN 1 END) as pub_15m,
        MAX(published_at) as last_published,
        round(EXTRACT(EPOCH FROM (now() - MAX(published_at)))) as sec_since_published
      FROM chapters
      WHERE published_at IS NOT NULL;
    `);
    report.pubs = pubs.rows[0];

    // 7. Recent claims in 15m
    const claims = await client.query(`
      SELECT 
        COUNT(CASE WHEN locked_at >= NOW() - INTERVAL '15 minutes' THEN 1 END) as claims_15m,
        COUNT(CASE WHEN updated_at >= NOW() - INTERVAL '15 minutes' AND status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' THEN 1 END) as completed_15m,
        COUNT(CASE WHEN updated_at >= NOW() - INTERVAL '15 minutes' AND status = 'FAILED' AND task_type = 'IMPORT_CHAPTER' THEN 1 END) as failed_15m
      FROM importer_queue;
    `);
    report.claims = claims.rows[0];

    console.log(JSON.stringify(report, null, 2));

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
