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

    // Top active queries
    const activeQ = await client.query(`
      SELECT pid, state, wait_event_type, wait_event,
             round(EXTRACT(EPOCH FROM (now() - query_start))::numeric, 2) as duration_sec,
             substring(query, 1, 150) as query_snip
      FROM pg_stat_activity
      WHERE datname = current_database() 
        AND state != 'idle' 
        AND pid != pg_backend_pid()
      ORDER BY duration_sec DESC;
    `);
    report.active_queries = activeQ.rows;

    // 3. Heartbeat & operational progress
    const hbRes = await client.query(`SELECT value FROM settings WHERE key = 'importer_heartbeat'`);
    report.heartbeat = hbRes.rows[0]?.value ? JSON.parse(hbRes.rows[0].value) : null;

    // 4. Timeline
    const timeline = await client.query(`
      SELECT 
        (SELECT MAX(locked_at) FROM importer_queue) as last_claim,
        (SELECT MAX(created_at) FROM importer_chapter_mappings) as last_stage,
        (SELECT MAX(published_at) FROM chapters WHERE published_at IS NOT NULL) as last_publish,
        (SELECT MAX(created_at) FROM chapters) as last_db_write
    `);
    report.timeline = timeline.rows[0];

    const nowEpoch = Date.now() / 1000;
    report.time_since = {
      since_last_claim_sec: timeline.rows[0].last_claim ? Math.round(nowEpoch - new Date(timeline.rows[0].last_claim).getTime()/1000) : null,
      since_last_stage_sec: timeline.rows[0].last_stage ? Math.round(nowEpoch - new Date(timeline.rows[0].last_stage).getTime()/1000) : null,
      since_last_publish_sec: timeline.rows[0].last_publish ? Math.round(nowEpoch - new Date(timeline.rows[0].last_publish).getTime()/1000) : null
    };

    // 5. Rates in last 5m and 15m
    const rates = await client.query(`
      SELECT 
        (SELECT count(*) FROM chapters WHERE published_at >= NOW() - INTERVAL '5 minutes') as pub_5m,
        (SELECT count(*) FROM chapters WHERE published_at >= NOW() - INTERVAL '15 minutes') as pub_15m,
        (SELECT count(*) FROM importer_chapter_mappings WHERE created_at >= NOW() - INTERVAL '5 minutes') as staged_5m,
        (SELECT count(*) FROM importer_chapter_mappings WHERE created_at >= NOW() - INTERVAL '15 minutes') as staged_15m,
        (SELECT count(*) FROM importer_queue WHERE updated_at >= NOW() - INTERVAL '5 minutes' AND status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER') as completed_5m,
        (SELECT count(*) FROM importer_queue WHERE updated_at >= NOW() - INTERVAL '15 minutes' AND status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER') as completed_15m,
        (SELECT count(*) FROM importer_queue WHERE locked_at >= NOW() - INTERVAL '5 minutes') as claims_5m,
        (SELECT count(*) FROM importer_queue WHERE locked_at >= NOW() - INTERVAL '15 minutes') as claims_15m
    `);
    report.rates = rates.rows[0];

    // 6. Currently importing jobs
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
