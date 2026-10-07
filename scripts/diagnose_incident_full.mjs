import pg from 'pg';
import fs from 'fs';
import crypto from 'crypto';

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
  max: 2,
  connectionTimeoutMillis: 10000
});

const BASE_URL = 'https://manga.project-nox-awerkori.workers.dev';
const TOKEN = 'FIzfNCLxC58nSriPHiXfo8PRYRz9zzKq';
const SECRET = 'prod-secret-9876543210-abcdef';
const sig = crypto.createHmac('sha256', SECRET).update(TOKEN).digest('base64');
const signedCookie = `${TOKEN}.${sig}`;
const cookieHeader = `better-auth.session_token=${signedCookie}; __Secure-better-auth.session_token=${signedCookie}`;

async function main() {
  const client = await pool.connect();
  const report = {};

  try {
    // 0. Sample Obra and Chapter for site probing
    const sampleObraRes = await client.query(`SELECT slug FROM works WHERE published = true ORDER BY updated_at DESC LIMIT 1`);
    const sampleObra = sampleObraRes.rows[0]?.slug || 'solo-leveling';
    const sampleChapRes = await client.query(`SELECT id FROM chapters WHERE published_at IS NOT NULL ORDER BY published_at DESC LIMIT 1`);
    const sampleChap = sampleChapRes.rows[0]?.id || '1';

    // 1. SITE MEASUREMENTS (3 requests each)
    const siteRoutes = [
      { name: 'HOME', path: '/' },
      { name: 'OBRA', path: `/obra/${sampleObra}` },
      { name: 'READER', path: `/ler/${sampleChap}` },
      { name: '/ME', path: '/me' },
      { name: '/ADMIN', path: '/admin' },
      { name: '/ADMIN/IMPORTER', path: '/admin/importer' }
    ];

    report.site = [];
    for (const r of siteRoutes) {
      const times = [];
      const ttfbs = [];
      let lastStatus = 0;
      for (let i = 0; i < 3; i++) {
        const start = performance.now();
        let ttfb = 0;
        try {
          const res = await fetch(`${BASE_URL}${r.path}`, {
            headers: {
              'Cookie': cookieHeader,
              'User-Agent': 'NoxAuditor/1.0',
              'Accept': 'text/html'
            },
            redirect: 'manual'
          });
          ttfb = performance.now() - start;
          await res.text();
          const total = performance.now() - start;
          times.push(total);
          ttfbs.push(ttfb);
          lastStatus = res.status;
        } catch (e) {
          times.push(9999);
          ttfbs.push(9999);
          lastStatus = 599;
        }
        await new Promise(res => setTimeout(res, 50));
      }
      times.sort((a,b) => a-b);
      ttfbs.sort((a,b) => a-b);
      report.site.push({
        route: r.name,
        path: r.path,
        status: lastStatus,
        ttfb_min: ttfbs[0].toFixed(1),
        ttfb_p50: ttfbs[1].toFixed(1),
        ttfb_max: ttfbs[2].toFixed(1),
        total_p50: times[1].toFixed(1),
        total_max: times[2].toFixed(1)
      });
    }

    // 2. YUGABYTE STATUS
    // YSQL connections
    const conns = await client.query(`
      SELECT 
        count(*) as total,
        count(CASE WHEN state = 'active' THEN 1 END) as active,
        count(CASE WHEN state = 'idle' THEN 1 END) as idle,
        count(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_tx
      FROM pg_stat_activity
      WHERE datname = current_database();
    `);
    report.ysql_connections = conns.rows[0];

    // Long running queries & locks
    const locksAndLong = await client.query(`
      SELECT pid, state, wait_event_type, wait_event,
             round(EXTRACT(EPOCH FROM (now() - query_start))::numeric, 2) as duration_sec,
             substring(query, 1, 150) as query_snip
      FROM pg_stat_activity
      WHERE datname = current_database() 
        AND state != 'idle' 
        AND pid != pg_backend_pid()
      ORDER BY duration_sec DESC;
    `);
    report.active_queries = locksAndLong.rows;

    // Top queries in pg_stat_statements
    const topStatements = await client.query(`
      SELECT substring(query, 1, 120) as query, calls,
             round(total_exec_time::numeric, 2) as total_ms,
             round(mean_exec_time::numeric, 2) as mean_ms,
             rows
      FROM pg_stat_statements
      ORDER BY total_exec_time DESC
      LIMIT 8;
    `);
    report.top_statements = topStatements.rows;

    // 3. IMPORTER OPERATIONAL TIMELINE
    const timeline = await client.query(`
      SELECT 
        (SELECT MAX(locked_at) FROM importer_queue) as last_claim,
        (SELECT MAX(created_at) FROM importer_chapter_mappings) as last_stage,
        (SELECT MAX(published_at) FROM chapters WHERE published_at IS NOT NULL) as last_publish,
        (SELECT MAX(created_at) FROM chapters) as last_db_write
    `);
    report.timeline = timeline.rows[0];

    // Calculate seconds since
    const nowEpoch = Date.now() / 1000;
    report.time_since = {
      since_last_claim_sec: timeline.rows[0].last_claim ? Math.round(nowEpoch - new Date(timeline.rows[0].last_claim).getTime()/1000) : null,
      since_last_stage_sec: timeline.rows[0].last_stage ? Math.round(nowEpoch - new Date(timeline.rows[0].last_stage).getTime()/1000) : null,
      since_last_publish_sec: timeline.rows[0].last_publish ? Math.round(nowEpoch - new Date(timeline.rows[0].last_publish).getTime()/1000) : null
    };

    // 4. QUEUE STATE & CLAIMABLE BREAKDOWN
    const qState = await client.query(`
      SELECT 
        status, task_type, count(*) as count
      FROM importer_queue
      GROUP BY status, task_type
      ORDER BY count DESC;
    `);
    report.queue_by_status = qState.rows;

    // Claimable now breakdown by priority / staff forced
    const claimable = await client.query(`
      SELECT 
        COUNT(CASE WHEN priority >= 100 THEN 1 END) as p0,
        COUNT(CASE WHEN priority >= 50 AND priority < 100 THEN 1 END) as p1,
        COUNT(CASE WHEN priority >= 20 AND priority < 50 THEN 1 END) as p2,
        COUNT(CASE WHEN priority < 20 THEN 1 END) as p3,
        COUNT(CASE WHEN q.task_type = 'IMPORT_CHAPTER' THEN 1 END) as import_chapters,
        COUNT(CASE WHEN q.task_type = 'SYNC_WORK' THEN 1 END) as sync_works,
        COUNT(CASE WHEN q.task_type = 'DISCOVER_WORKS' THEN 1 END) as discover_works
      FROM importer_queue q
      WHERE q.status = 'QUEUED'
        AND (q.next_run_at IS NULL OR q.next_run_at <= NOW());
    `);
    report.claimable_now = claimable.rows[0];

    // Staff forced pending requests
    const staffForced = await client.query(`
      SELECT count(*) as pending_staff_requests
      FROM importer_staff_requests
      WHERE status = 'PENDING';
    `);
    report.pending_staff_requests = staffForced.rows[0].pending_staff_requests;

    // 5. STAGED / PUBLICATION BARRIER
    const stagedOverview = await client.query(`
      SELECT 
        count(*) as staged_total,
        count(DISTINCT work_id) as staged_works,
        min(created_at) as oldest_staged,
        max(created_at) as newest_staged
      FROM importer_chapter_mappings
      WHERE status = 'STAGED';
    `);
    report.staged_overview = stagedOverview.rows[0];

    // Top blocked works by STAGED barrier
    const topBlocked = await client.query(`
      WITH staged_agg AS (
        SELECT 
          work_id,
          count(*) as staged_count,
          min(chapter_sort_key) as min_staged_sort,
          max(chapter_sort_key) as max_staged_sort
        FROM importer_chapter_mappings
        WHERE status = 'STAGED'
        GROUP BY work_id
      ),
      published_agg AS (
        SELECT 
          work_id,
          max(number) as max_pub_sort
        FROM chapters
        WHERE published_at IS NOT NULL
        GROUP BY work_id
      )
      SELECT 
        w.title,
        w.slug,
        sa.staged_count,
        COALESCE(pa.max_pub_sort, 0) as published_up_to,
        sa.min_staged_sort as min_staged,
        (sa.min_staged_sort - COALESCE(pa.max_pub_sort, 0)) as diff_to_frontier
      FROM staged_agg sa
      JOIN works w ON w.id = sa.work_id
      LEFT JOIN published_agg pa ON pa.work_id = sa.work_id
      ORDER BY sa.staged_count DESC
      LIMIT 10;
    `);
    report.top_staged_blockers = topBlocked.rows;

    // 6. PIPELINE LAST 15M RATES
    const rates15m = await client.query(`
      SELECT 
        (SELECT count(*) FROM chapters WHERE published_at >= NOW() - INTERVAL '15 minutes') as pub_15m,
        (SELECT count(*) FROM chapters WHERE published_at >= NOW() - INTERVAL '5 minutes') as pub_5m,
        (SELECT count(*) FROM importer_chapter_mappings WHERE created_at >= NOW() - INTERVAL '15 minutes') as staged_15m,
        (SELECT count(*) FROM importer_queue WHERE updated_at >= NOW() - INTERVAL '15 minutes' AND status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER') as completed_15m,
        (SELECT count(*) FROM importer_queue WHERE updated_at >= NOW() - INTERVAL '15 minutes' AND status = 'FAILED' AND task_type = 'IMPORT_CHAPTER') as failed_15m,
        (SELECT count(*) FROM importer_queue WHERE locked_at >= NOW() - INTERVAL '15 minutes') as claims_15m
    `);
    report.rates_15m = rates15m.rows[0];

    // 7. Active workers from queue (currently IMPORTING / locked)
    const activeJobs = await client.query(`
      SELECT 
        id, task_type, priority, locked_by, 
        round(EXTRACT(EPOCH FROM (now() - locked_at))::numeric, 1) as locked_sec,
        payload->>'workId' as work_id,
        payload->>'source' as source,
        payload->>'chapterNumber' as chapter_num
      FROM importer_queue
      WHERE status = 'IMPORTING'
      ORDER BY locked_at ASC;
    `);
    report.active_jobs = activeJobs.rows;

    console.log(JSON.stringify(report, null, 2));
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
