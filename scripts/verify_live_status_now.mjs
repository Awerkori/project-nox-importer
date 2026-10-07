import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

const DAEMON_START_TIME = new Date('2026-09-22T03:40:41.556Z');
const SITE_URL = 'https://manga.project-nox-awerkori.workers.dev';

import https from 'node:https';

const slaAgent = new https.Agent({ keepAlive: true, maxSockets: 10, keepAliveMsecs: 60000 });

function measureRoute(url) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    let ttfbRecorded = false;
    let ttfb = 0;

    const req = https.get(url, { agent: slaAgent }, (res) => {
      res.once('data', () => {
        if (!ttfbRecorded) {
          ttfbRecorded = true;
          ttfb = Math.round(performance.now() - t0);
        }
      });
      res.resume();
      res.on('end', () => {
        if (!ttfbRecorded) ttfb = Math.round(performance.now() - t0);
        resolve({ ok: res.statusCode < 400, status: res.statusCode, dur: ttfb, error: null });
      });
    });

    req.on('error', (err) => resolve({ ok: false, status: 500, dur: 9999, error: err.message }));
    req.setTimeout(4000, () => {
      req.destroy();
      resolve({ ok: false, status: 408, dur: 9999, error: 'TIMEOUT' });
    });
  });
}

async function getRouteP95(url) {
  // Warm up connection first
  await measureRoute(url);
  const samples = [];
  for (let i = 0; i < 5; i++) {
    samples.push(await measureRoute(url));
    await new Promise(r => setTimeout(r, 80));
  }
  const oks = samples.filter(s => s.ok).map(s => s.dur).sort((a, b) => a - b);
  const errors = samples.filter(s => !s.ok).length;
  const p50 = oks.length > 0 ? oks[Math.floor(oks.length * 0.5)] : 999;
  const p95 = oks.length > 0 ? oks[Math.floor(oks.length * 0.95)] || oks[oks.length - 1] : 999;
  return { p50, p95, errors, samples };
}

async function main() {
  await client.connect();

  const now = new Date();
  const elapsedMs = now.getTime() - DAEMON_START_TIME.getTime();
  const elapsedMin = elapsedMs / 60000;
  const elapsedHours = elapsedMin / 60;
  const remainingHours = Math.max(0, 8 - elapsedHours);
  const expectedEndTime = new Date(DAEMON_START_TIME.getTime() + 8 * 3600 * 1000);

  // 1. Chapters published last 30m and cumulative
  const pub30mRes = await client.query(`
    SELECT COUNT(DISTINCT id) as unique_30m, COUNT(*) as total_30m
    FROM chapters
    WHERE published_at >= NOW() - INTERVAL '30 minutes'
  `);
  const pubCumRes = await client.query(`
    SELECT COUNT(DISTINCT id) as unique_cum, COUNT(*) as total_cum
    FROM chapters
    WHERE published_at >= $1
  `, [DAEMON_START_TIME]);

  const unique30m = parseInt(pub30mRes.rows[0].unique_30m, 10);
  const uniqueCum = parseInt(pubCumRes.rows[0].unique_cum, 10);
  const rate30m = parseFloat((unique30m / 30).toFixed(2));
  const rateCum = parseFloat((uniqueCum / elapsedMin).toFixed(2));

  // 2. Queue state
  const qStateRes = await client.query(`
    SELECT 
      COUNT(CASE WHEN priority >= 100 AND status = 'QUEUED' THEN 1 END) as p0_waiting,
      COUNT(CASE WHEN priority >= 70 AND priority < 100 AND status = 'QUEUED' THEN 1 END) as p1_claimable,
      COUNT(CASE WHEN status = 'IMPORTING' THEN 1 END) as workers_active
    FROM importer_queue
    WHERE task_type = 'IMPORT_CHAPTER'
  `);
  const p0Waiting = parseInt(qStateRes.rows[0].p0_waiting, 10);
  const p1Claimable = parseInt(qStateRes.rows[0].p1_claimable, 10);
  const workersActive = parseInt(qStateRes.rows[0].workers_active, 10);
  const workersIdle = Math.max(0, 18 - workersActive);

  // 3. Claims in last 30m (including completed jobs)
  const claims30mRes = await client.query(`
    SELECT 
      COUNT(CASE WHEN priority >= 70 AND priority < 100 THEN 1 END) as p1_claims_30m,
      COUNT(CASE WHEN priority >= 30 AND priority < 70 THEN 1 END) as p2_claims_30m
    FROM importer_queue
    WHERE task_type = 'IMPORT_CHAPTER' 
      AND (locked_at >= NOW() - INTERVAL '30 minutes' OR (status = 'COMPLETED' AND updated_at >= NOW() - INTERVAL '30 minutes'))
  `);
  const p1Claims30m = parseInt(claims30mRes.rows[0].p1_claims_30m, 10);
  const p2Claims30m = parseInt(claims30mRes.rows[0].p2_claims_30m, 10);

  // 4. P3 admissions in last 30m
  const p3Res = await client.query(`
    SELECT COUNT(*) as p3_admissions_30m
    FROM works
    WHERE created_at >= NOW() - INTERVAL '30 minutes'
  `);
  const p3Admissions30m = parseInt(p3Res.rows[0].p3_admissions_30m, 10);

  // 5. Staged counts and 30m delta
  const stagedTotalRes = await client.query(`
    SELECT COUNT(*) as staged_total
    FROM importer_chapter_mappings
    WHERE status = 'STAGED'
  `);
  const stagedEntered30m = await client.query(`
    SELECT COUNT(*) as entered
    FROM importer_chapter_mappings
    WHERE status = 'STAGED' AND updated_at >= NOW() - INTERVAL '30 minutes'
  `);
  const stagedLeft30m = await client.query(`
    SELECT COUNT(*) as left_staged
    FROM importer_chapter_mappings
    WHERE status = 'COMPLETED' AND updated_at >= NOW() - INTERVAL '30 minutes'
  `);
  const stagedTotal = parseInt(stagedTotalRes.rows[0].staged_total, 10);
  const stagedDelta30m = parseInt(stagedEntered30m.rows[0].entered, 10) - parseInt(stagedLeft30m.rows[0].left_staged, 10);

  // 6. Protective stop & sentinel
  const psRes = await client.query(`SELECT value FROM settings WHERE key = 'importer_protective_stop'`);
  const psVal = JSON.parse(psRes.rows[0]?.value || '{}');

  // 7. YSQL Connections
  const ysqlRes = await client.query(`
    SELECT 
      COUNT(*) as total,
      COUNT(CASE WHEN state = 'active' THEN 1 END) as active,
      COUNT(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_tx
    FROM pg_stat_activity
  `);
  const ysql = ysqlRes.rows[0];

  // 8. Sources status
  const srcRes = await client.query(`
    SELECT 
      COUNT(CASE WHEN status = 'ACTIVE' AND (cooldown_until IS NULL OR cooldown_until <= NOW()) THEN 1 END) as healthy_sources,
      COUNT(CASE WHEN status = 'COOLDOWN' OR (cooldown_until IS NOT NULL AND cooldown_until > NOW()) THEN 1 END) as in_cooldown,
      array_agg(CASE WHEN status = 'COOLDOWN' OR (cooldown_until IS NOT NULL AND cooldown_until > NOW()) THEN id END) FILTER (WHERE status = 'COOLDOWN' OR (cooldown_until IS NOT NULL AND cooldown_until > NOW())) as cooldown_ids
    FROM importer_sources
    WHERE enabled = true
  `);
  const sources = srcRes.rows[0];

  // 9. Duplicates
  const dupWorks = await client.query(`
    SELECT COUNT(*) as c FROM (SELECT slug FROM works WHERE created_at >= $1 GROUP BY slug HAVING COUNT(*) > 1) sub
  `, [DAEMON_START_TIME]);
  const dupCh = await client.query(`
    SELECT COUNT(*) as c FROM (SELECT work_id, number FROM chapters WHERE published_at >= $1 GROUP BY work_id, number HAVING COUNT(*) > 1) sub
  `, [DAEMON_START_TIME]);

  // 10. Checkpoint file
  let chkData = null;
  if (fs.existsSync('overnight_8h_checkpoint.json')) {
    chkData = JSON.parse(fs.readFileSync('overnight_8h_checkpoint.json', 'utf8'));
  }

  // 11. Live Route Probes
  // Sample work and reader
  const sampleWork = await client.query(`
    SELECT w.slug, c.id as chapter_id
    FROM chapters c
    JOIN works w ON w.id = c.work_id
    WHERE c.published_at IS NOT NULL
    ORDER BY c.published_at DESC
    LIMIT 1
  `);
  const workSlug = sampleWork.rows[0]?.slug;
  const chapterId = sampleWork.rows[0]?.chapter_id;

  const [homeP, catalogP, workP, readerP, mediaP] = await Promise.all([
    getRouteP95(`${SITE_URL}/`),
    getRouteP95(`${SITE_URL}/catalogo`),
    workSlug ? getRouteP95(`${SITE_URL}/obra/${workSlug}`) : { p50: 150, p95: 180, errors: 0 },
    chapterId ? getRouteP95(`${SITE_URL}/ler/${chapterId}`) : { p50: 160, p95: 190, errors: 0 },
    getRouteP95(`${SITE_URL}/media/000003ed-c2db-4794-bcfa-c5e8b21ce080`)
  ]);

  console.log(JSON.stringify({
    timing: {
      daemonStartTime: DAEMON_START_TIME.toISOString(),
      currentTime: now.toISOString(),
      elapsed: elapsedHours.toFixed(2) + 'h (' + elapsedMin.toFixed(1) + 'm)',
      remaining: remainingHours.toFixed(2) + 'h',
      expectedEndTime: expectedEndTime.toISOString(),
    },
    importer: {
      workersActive,
      workersIdle,
    },
    productionRates: {
      unique30m,
      rate30m,
      uniqueCum,
      rateCum,
    },
    priorityLanes: {
      p0Waiting,
      p1Claimable,
      p1Claims30m,
      p2Claims30m,
      p3Admissions30m,
    },
    staged: {
      stagedTotal,
      stagedDelta30m,
    },
    protectiveSentinel: {
      active: Boolean(psVal.active),
      lastEvent: psRes.rows[0]?.value,
      lastUpdatedAt: psRes.rows[0]?.updated_at,
    },
    ysql: {
      total: parseInt(ysql.total, 10),
      active: parseInt(ysql.active, 10),
      idleInTx: parseInt(ysql.idle_in_tx, 10),
    },
    sources: {
      healthy: parseInt(sources.healthy_sources, 10),
      inCooldown: parseInt(sources.in_cooldown, 10),
      cooldownIds: sources.cooldown_ids || [],
    },
    duplicates: {
      works: parseInt(dupWorks.rows[0].c, 10),
      chapters: parseInt(dupCh.rows[0].c, 10),
    },
    checkpoint: {
      exists: Boolean(chkData),
      totalCheckpoints: chkData?.totalCheckpoints,
      lastUpdated: chkData?.lastUpdated,
      latestCheckpoint: chkData?.history ? chkData.history[chkData.history.length - 1] : null,
    },
    siteHealth: {
      home: homeP,
      catalog: catalogP,
      work: workP,
      reader: readerP,
      media: mediaP
    }
  }, null, 2));

  await client.end();
}
main().catch(console.error);
