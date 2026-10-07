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
  const now = new Date();
  const nowIso = now.toISOString();

  // 1. Chapters fresh
  const freshRes = await client.query(`
    SELECT id, work_id, number, published_at
    FROM chapters
    WHERE published_at IS NOT NULL
    ORDER BY published_at DESC
    LIMIT 5
  `);
  const lastFresh = freshRes.rows[0];
  const lastFreshAt = lastFresh?.published_at ? new Date(lastFresh.published_at).toISOString() : null;
  const lastFreshAgeSec = lastFreshAt ? Math.round((now.getTime() - new Date(lastFreshAt).getTime()) / 1000) : null;

  // Fresh 5m and 30m
  const f5Res = await client.query(`
    SELECT count(*) as count
    FROM chapters
    WHERE published_at >= NOW() - INTERVAL '5 minutes'
  `);
  const f30Res = await client.query(`
    SELECT count(*) as count
    FROM chapters
    WHERE published_at >= NOW() - INTERVAL '30 minutes'
  `);

  // 2. Queue jobs: last started, last completed
  const lastStartedRes = await client.query(`
    SELECT id, source, task_type, locked_at, created_at
    FROM importer_queue
    WHERE locked_at IS NOT NULL
    ORDER BY locked_at DESC
    LIMIT 1
  `);
  const lastStarted = lastStartedRes.rows[0];

  const lastCompletedRes = await client.query(`
    SELECT id, source, task_type, updated_at
    FROM importer_queue
    WHERE status = 'COMPLETED'
    ORDER BY updated_at DESC
    LIMIT 1
  `);
  const lastCompleted = lastCompletedRes.rows[0];

  // 3. Queue statuses
  const queueCountsRes = await client.query(`
    SELECT status, count(*) as count
    FROM importer_queue
    GROUP BY status
  `);
  const queueCounts = Object.fromEntries(queueCountsRes.rows.map(r => [r.status, parseInt(r.count, 10)]));

  // Staged count
  const stagedRes = await client.query(`
    SELECT count(*) as count
    FROM importer_chapter_mappings
    WHERE status = 'STAGED'
  `);
  const stagedCount = parseInt(stagedRes.rows[0].count, 10);

  // Expired leases
  const expiredLeasesRes = await client.query(`
    SELECT count(*) as count
    FROM importer_queue
    WHERE status = 'IMPORTING' AND lease_expires_at < NOW()
  `);
  const expiredLeasesCount = parseInt(expiredLeasesRes.rows[0].count, 10);

  // Source cooldowns
  const cooldownsRes = await client.query(`
    SELECT id, name, status, cooldown_until, blocked_reason
    FROM importer_sources
    WHERE cooldown_until > NOW()
  `);

  // Settings: manual_stop, protective stop
  const settingsRes = await client.query(`
    SELECT key, value FROM settings WHERE key IN ('importer_emergency_stop', 'importer_manual_stop', 'publication_barrier', 'importer_protective_stop')
  `);
  const settings = Object.fromEntries(settingsRes.rows.map(r => [r.key, r.value]));

  // Telemetry: latest adaptive telemetry
  let latestTelem = null;
  let telemRows = [];
  try {
    const telemRes = await client.query(`
      SELECT *
      FROM importer_telemetry
      ORDER BY created_at DESC
      LIMIT 10
    `);
    telemRows = telemRes.rows;
    latestTelem = telemRows[0];
  } catch (e) {
    console.error('importer_telemetry error:', e.message);
  }

  // YSQL active / total
  const ysqlRes = await client.query(`
    SELECT state, count(*) as count
    FROM pg_stat_activity
    GROUP BY state
  `);
  const ysqlCounts = Object.fromEntries(ysqlRes.rows.map(r => [r.state || 'internal/background', parseInt(r.count, 10)]));
  const ysqlTotal = Object.values(ysqlCounts).reduce((a, b) => a + b, 0);

  // 4. Measure Site Latencies (Home & Reader)
  const probe = async (url) => {
    const times = [];
    let err5xx = 0;
    for (let i = 0; i < 3; i++) {
      const t0 = Date.now();
      try {
        const resp = await fetch(url, { headers: { 'User-Agent': 'Nox-Probe/1.0' }, signal: AbortSignal.timeout(4000) });
        times.push(Date.now() - t0);
        if (resp.status >= 500) err5xx++;
      } catch {
        times.push(4000);
        err5xx++;
      }
    }
    times.sort((a, b) => a - b);
    return {
      p50: times[1],
      p95: times[2],
      err5xx
    };
  };

  const homeLatency = await probe('https://manga.project-nox-awerkori.workers.dev/');
  // find a sample chapter for reader
  const sampleChapter = freshRes.rows[0]?.id || '1';
  const readerLatency = await probe(`https://manga.project-nox-awerkori.workers.dev/ler/${sampleChapter}`);

  console.log(JSON.stringify({
    SNAPSHOT_AT: nowIso,
    LAST_FRESH_AT: lastFreshAt,
    LAST_FRESH_AGE_SECONDS: lastFreshAgeSec,
    LAST_FRESH_AGE_MINUTES: lastFreshAgeSec ? (lastFreshAgeSec / 60).toFixed(1) : null,
    LAST_STARTED: lastStarted ? { id: lastStarted.id, source: lastStarted.source, locked_at: lastStarted.locked_at } : null,
    LAST_COMPLETED: lastCompleted ? { id: lastCompleted.id, source: lastCompleted.source, updated_at: lastCompleted.updated_at } : null,
    FRESH_5M: parseInt(f5Res.rows[0].count, 10),
    FRESH_30M: parseInt(f30Res.rows[0].count, 10),
    CURRENT_CAPACITY: latestTelem?.concurrency ?? null,
    MAX_CAPACITY: 8,
    ADAPTIVE_STATE: latestTelem?.cycle_action ?? null,
    PRESSURE_REASON: latestTelem?.cycle_reason ?? null,
    MANUAL_STOP: settings.importer_manual_stop === 'true' || settings.importer_emergency_stop === 'true',
    AUTO_EMERGENCY_PAUSE: false,
    EFFECTIVE_CLAIMABLE: (queueCounts.QUEUED || 0) + (queueCounts.RETRY || 0),
    IMPORTING: queueCounts.IMPORTING || 0,
    QUEUED_HOT: queueCounts.QUEUED || 0,
    RETRY: queueCounts.RETRY || 0,
    STAGED: stagedCount,
    EXPIRED_LEASES: expiredLeasesCount,
    SOURCE_COOLDOWNS: cooldownsRes.rows,
    EVENT_LOOP_P50: latestTelem?.event_loop_lag_ms ?? null,
    EVENT_LOOP_P95: null,
    RSS: latestTelem?.rss_mb ? `${latestTelem.rss_mb} MB` : null,
    HEAP: latestTelem?.heap_used_mb ? `${latestTelem.heap_used_mb} MB` : null,
    SITE_HOME_P50: homeLatency.p50,
    SITE_HOME_P95: homeLatency.p95,
    SITE_READER_P50: readerLatency.p50,
    SITE_READER_P95: readerLatency.p95,
    HTTP_5XX: homeLatency.err5xx + readerLatency.err5xx,
    YSQL_ACTIVE: ysqlCounts.active || 0,
    YSQL_TOTAL: ysqlTotal
  }, null, 2));

  await client.end();
}

main().catch(console.error);
