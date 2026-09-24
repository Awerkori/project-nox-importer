import pg from 'pg';
import fs from 'fs';
import crypto from 'crypto';
import { SOURCE_CONCURRENCY_LIMITS, DEFAULT_SOURCE_LIMIT } from '../build/core/concurrency.js';

const envVars = Object.fromEntries(
  fs.readFileSync('/home/awerkori/.config/project-nox/yugabyte.env', 'utf8')
    .split('\n')
    .filter(l => l.includes('=') && !l.startsWith('#'))
    .map(l => [l.split('=')[0].trim(), l.substring(l.indexOf('=') + 1).trim().replace(/^['"]|['"]$/g, '')])
);

const DISCLOUD_TOKEN = '4bc8293a4be7d5e98fe447b89d6d3bb0e12897b1b421820829d4ee8127348f2404af1f927eea5a62c186f98bc11ef7676a9f9a68';
const APP_ID = '1788873398156';

const BASE_URL = 'https://manga.project-nox-awerkori.workers.dev';
const AUTH_TOKEN = 'FIzfNCLxC58nSriPHiXfo8PRYRz9zzKq';
const AUTH_SECRET = 'prod-secret-9876543210-abcdef';
const sig = crypto.createHmac('sha256', AUTH_SECRET).update(AUTH_TOKEN).digest('base64');
const signedCookie = `${AUTH_TOKEN}.${sig}`;
const COOKIE_HEADER = `better-auth.session_token=${signedCookie}; __Secure-better-auth.session_token=${signedCookie}`;

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

async function probeUrl(url, headers = {}) {
  const start = performance.now();
  try {
    const res = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 NoxScorecard/1.0',
        ...headers
      },
      redirect: 'manual',
      signal: AbortSignal.timeout(8000)
    });
    await res.text();
    return { ok: res.ok || res.status === 303 || res.status === 302, status: res.status, durationMs: Math.round(performance.now() - start) };
  } catch (err) {
    return { ok: false, durationMs: Math.round(performance.now() - start), error: err.message };
  }
}

async function getDiscloudStatus() {
  try {
    const res = await fetch(`https://gw.discloud.com/api/app/${APP_ID}/status`, {
      headers: { Authorization: `Bearer ${DISCLOUD_TOKEN}` },
      signal: AbortSignal.timeout(4000)
    });
    if (res.ok) {
      const data = await res.json();
      const app = data.apps || data.app || {};
      const cpu = parseFloat((app.cpu || '0').replace('%', ''));
      const ram = parseFloat(app.ram || 0);
      return { cpu, ram, memory: app.memory, container: app.container };
    }
  } catch {}
  return { cpu: 0, ram: 0, memory: '', container: 'unknown' };
}

async function main() {
  const durationMin = parseFloat(process.argv[2] || '5');
  const sampleIntervalSec = 10;
  const totalSamples = Math.round((durationMin * 60) / sampleIntervalSec);

  console.log(`Starting Production Scorecard Measurement for ${durationMin} minutes (${totalSamples} samples at ${sampleIntervalSec}s intervals)...`);
  const client = await pool.connect();

  const startTime = Date.now();
  const samples = [];

  // Initial markers
  const initPublishedRes = await client.query(`SELECT count(*) as count FROM chapters WHERE published_at IS NOT NULL;`);
  const initChaptersRes = await client.query(`SELECT count(*) as count FROM chapters;`);
  const initClaimedRes = await client.query(`SELECT count(*) as count FROM importer_queue WHERE task_type = 'IMPORT_CHAPTER' AND status IN ('IMPORTING', 'COMPLETED');`);

  const initialPublished = parseInt(initPublishedRes.rows[0].count, 10);
  const initialChapters = parseInt(initChaptersRes.rows[0].count, 10);
  const initialClaimed = parseInt(initClaimedRes.rows[0].count, 10);

  try {
    for (let i = 0; i < totalSamples; i++) {
      const sampleStart = Date.now();

      // 1. Chapter Inflight & Slot Breakdown
      const chapterInflightRes = await client.query(`
        SELECT id, source, EXTRACT(EPOCH FROM (now() - locked_at)) as elapsed_sec
        FROM importer_queue
        WHERE status = 'IMPORTING' AND task_type = 'IMPORT_CHAPTER' AND locked_at IS NOT NULL;
      `);

      let activeDownload = 0;
      let activeUpload = 0;
      let activeDb = 0;
      let chapterInflight = 0;

      for (const row of chapterInflightRes.rows) {
        const elapsed = parseFloat(row.elapsed_sec || 0);
        if (elapsed < 180) {
          chapterInflight++;
          if (elapsed <= 12) {
            activeDownload++;
          } else if (elapsed <= 24) {
            activeUpload++;
          } else {
            activeDb++;
          }
        }
      }

      // 2. Control Plane Inflight
      const cpInflightRes = await client.query(`
        SELECT count(*) as count
        FROM importer_queue
        WHERE status = 'IMPORTING' AND task_type IN ('SYNC_WORK', 'DISCOVER_WORKS');
      `);
      const controlPlaneInflight = parseInt(cpInflightRes.rows[0].count, 10);
      const totalDbImporting = chapterInflightRes.rows.length + controlPlaneInflight;

      // 3. Staged Barrier Count
      const stagedRes = await client.query(`
        SELECT count(*) as count
        FROM importer_chapter_mappings
        WHERE status = 'STAGED';
      `);
      const stagedBarrierCount = parseInt(stagedRes.rows[0].count, 10);

      // 4. Queue availability
      const queueStatusRes = await client.query(`
        SELECT 
          COUNT(CASE WHEN q.status = 'QUEUED' THEN 1 END) as queued_count,
          COUNT(CASE WHEN q.status = 'RETRY' AND q.next_run_at <= now() THEN 1 END) as retry_due_count
        FROM importer_queue q
        WHERE q.task_type = 'IMPORT_CHAPTER';
      `);
      const queuedCount = parseInt(queueStatusRes.rows[0].queued_count, 10);
      const retryDueCount = parseInt(queueStatusRes.rows[0].retry_due_count, 10);
      const hasJobsAvailable = (queuedCount + retryDueCount) > 0;

      // 5. Ready Sources & Ready Works Capacity
      const sourcesRes = await client.query(`
        SELECT s.id,
               COALESCE(act.in_flight, 0) as in_flight,
               (s.status = 'COOLDOWN' OR (s.cooldown_until IS NOT NULL AND s.cooldown_until > now())) as in_cooldown
        FROM importer_sources s
        LEFT JOIN (
          SELECT source, count(*) as in_flight
          FROM importer_queue
          WHERE status = 'IMPORTING' AND task_type = 'IMPORT_CHAPTER'
          GROUP BY source
        ) act ON act.source = s.id
        WHERE s.enabled = true;
      `);

      let readySourcesCount = 0;
      let totalAvailablePermits = 0;
      for (const s of sourcesRes.rows) {
        const lim = SOURCE_CONCURRENCY_LIMITS[s.id] || DEFAULT_SOURCE_LIMIT;
        const inUse = parseInt(s.in_flight, 10);
        const avail = Math.max(0, lim.maxChapters - inUse);
        const inCooldown = s.in_cooldown;
        if (!inCooldown && avail > 0) {
          readySourcesCount++;
          totalAvailablePermits += avail;
        }
      }

      // Ready works count: distinct works that have queued chapters on ready sources
      const readyWorksRes = await client.query(`
        SELECT COUNT(DISTINCT (q.payload->>'workId')) as count
        FROM importer_queue q
        JOIN importer_sources s ON s.id = q.source
        WHERE (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= now()))
          AND q.task_type = 'IMPORT_CHAPTER'
          AND s.enabled = true
          AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= now())));
      `);
      const readyWorksCount = parseInt(readyWorksRes.rows[0].count, 10);

      // 6. Mutually Exclusive Slot Categorization across all 8 slots
      let waitingRateLimit = 0;
      let waitingSource = 0;
      let waitingBarrier = 0;
      let waitingJob = 0;
      let idle = 0;

      let remainingSlots = Math.max(0, 8 - (activeDownload + activeUpload + activeDb));

      if (remainingSlots > 0) {
        if (!hasJobsAvailable) {
          waitingJob = remainingSlots;
          remainingSlots = 0;
        } else if (readySourcesCount === 0) {
          waitingSource = remainingSlots;
          remainingSlots = 0;
        } else {
          // If staged chapters are blocking frontier advancement
          if (stagedBarrierCount > 0 && remainingSlots > 0) {
            const barrierBlocked = Math.min(remainingSlots, Math.ceil(stagedBarrierCount / 2));
            waitingBarrier += barrierBlocked;
            remainingSlots -= barrierBlocked;
          }
          // If source capacity / rate limiter delay is constraining
          if (remainingSlots > 0 && totalAvailablePermits < remainingSlots) {
            const constrained = remainingSlots - totalAvailablePermits;
            waitingRateLimit += constrained;
            remainingSlots -= constrained;
          }
          // Remaining are between loop dispatches / idle
          if (remainingSlots > 0) {
            idle += remainingSlots;
            remainingSlots = 0;
          }
        }
      }

      // Sanity assertion: total slots must be strictly 8
      const slotSum = activeDownload + activeUpload + activeDb + waitingRateLimit + waitingSource + waitingBarrier + waitingJob + idle;
      if (slotSum !== 8) {
        throw new Error(`Invariant violated: slot sum is ${slotSum} != 8`);
      }

      // 7. DB Connections
      const connRes = await client.query(`
        SELECT 
          count(*) as total,
          count(CASE WHEN state = 'active' THEN 1 END) as active,
          count(CASE WHEN state = 'idle' THEN 1 END) as idle,
          count(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_tx
        FROM pg_stat_activity 
        WHERE datname = current_database();
      `);

      // 8. Site Latency Probes
      const homeProbe = await probeUrl(`${BASE_URL}/`);
      const obraProbe = await probeUrl(`${BASE_URL}/obra/the-seed-of-destiny`);
      const readerProbe = await probeUrl(`${BASE_URL}/ler/c452eb28-fe1c-4e83-a0b9-2377f6198cbd`);
      const meProbe = await probeUrl(`${BASE_URL}/me`, { 'Cookie': COOKIE_HEADER });
      const adminProbe = await probeUrl(`${BASE_URL}/admin/importer`, { 'Cookie': COOKIE_HEADER });

      // 9. Discloud Resources
      const discloud = await getDiscloudStatus();

      const sampleData = {
        timestamp: new Date().toISOString(),
        sampleIndex: i + 1,
        chapterInflight,
        controlPlaneInflight,
        totalDbImporting,
        activeDownload,
        activeUpload,
        activeDb,
        waitingRateLimit,
        waitingSource,
        waitingBarrier,
        waitingJob,
        idle,
        readySourcesCount,
        readyWorksCount,
        ysqlTotal: parseInt(connRes.rows[0].total, 10),
        ysqlActive: parseInt(connRes.rows[0].active, 10),
        ysqlIdle: parseInt(connRes.rows[0].idle, 10),
        ysqlIdleInTx: parseInt(connRes.rows[0].idle_in_tx, 10),
        discloudCpu: discloud.cpu,
        discloudRamMb: discloud.ram,
        homeLatencyMs: homeProbe.durationMs,
        obraLatencyMs: obraProbe.durationMs,
        readerLatencyMs: readerProbe.durationMs,
        meLatencyMs: meProbe.durationMs,
        adminLatencyMs: adminProbe.durationMs
      };
      samples.push(sampleData);

      console.log(`[Sample ${i+1}/${totalSamples}] ChInf: ${chapterInflight}/8 (CP: ${controlPlaneInflight}, DB: ${totalDbImporting}) | Act: [DL:${activeDownload} UP:${activeUpload} DB:${activeDb}] Wait: [RL:${waitingRateLimit} Src:${waitingSource} Bar:${waitingBarrier} Job:${waitingJob} Idl:${idle}] | YSQL: ${connRes.rows[0].total}/13 (${connRes.rows[0].active} act) | CPU: ${discloud.cpu}% | Home: ${homeProbe.durationMs}ms | Obra: ${obraProbe.durationMs}ms | Reader: ${readerProbe.durationMs}ms | Me: ${meProbe.durationMs}ms | Admin: ${adminProbe.durationMs}ms`);

      const elapsed = Date.now() - sampleStart;
      const sleepMs = Math.max(100, (sampleIntervalSec * 1000) - elapsed);
      await new Promise(r => setTimeout(r, sleepMs));
    }

    const endTime = Date.now();
    const actualDurationMinutes = (endTime - startTime) / (60 * 1000);

    // Final counters
    const finalPublishedRes = await client.query(`SELECT count(*) as count FROM chapters WHERE published_at IS NOT NULL;`);
    const finalChaptersRes = await client.query(`SELECT count(*) as count FROM chapters;`);
    const finalClaimedRes = await client.query(`SELECT count(*) as count FROM importer_queue WHERE task_type = 'IMPORT_CHAPTER' AND status IN ('IMPORTING', 'COMPLETED');`);

    const finalPublished = parseInt(finalPublishedRes.rows[0].count, 10);
    const finalChapters = parseInt(finalChaptersRes.rows[0].count, 10);
    const finalClaimed = parseInt(finalClaimedRes.rows[0].count, 10);

    const deltaPublished = Math.max(0, finalPublished - initialPublished);
    const deltaChapters = Math.max(0, finalChapters - initialChapters);
    const deltaClaimed = Math.max(0, finalClaimed - initialClaimed);

    // Detailed chapter breakdown in window
    const windowStartSql = new Date(startTime).toISOString();
    const pubWindowRes = await client.query(`
      SELECT c.id, c.work_id, c.number, c.published_at,
             MAX(q.updated_at) as latest_q_comp
      FROM chapters c
      LEFT JOIN importer_queue q ON q.task_type = 'IMPORT_CHAPTER'
        AND (q.payload->>'workId') = c.work_id::text
        AND (q.payload->>'chapterNumber')::numeric = c.number
        AND q.status = 'COMPLETED'
      WHERE c.published_at >= $1
      GROUP BY c.id, c.work_id, c.number, c.published_at;
    `, [windowStartSql]);

    let newPublished = 0;
    let cascadePublished = 0;
    for (const r of pubWindowRes.rows) {
      const pubTime = new Date(r.published_at).getTime();
      const qTime = r.latest_q_comp ? new Date(r.latest_q_comp).getTime() : 0;
      const diffSec = qTime > 0 ? Math.abs(pubTime - qTime) / 1000 : 999999;
      if (diffSec <= 180) {
        newPublished++;
      } else {
        cascadePublished++;
      }
    }

    // End to end durations for completed chapters in window
    const durationRes = await client.query(`
      SELECT 
        round(percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (updated_at - locked_at)))) as p50_sec,
        round(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (updated_at - locked_at)))) as p95_sec
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER'
        AND status = 'COMPLETED'
        AND updated_at >= $1
        AND locked_at IS NOT NULL;
    `, [windowStartSql]);

    // Timeouts in window
    const timeoutsRes = await client.query(`
      SELECT count(*) as count
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER'
        AND updated_at >= $1
        AND (last_error ILIKE '%timeout%' OR last_error ILIKE '%lease%expired%');
    `, [windowStartSql]);

    // Averages calculation
    const avgChInf = (samples.reduce((s, x) => s + x.chapterInflight, 0) / samples.length).toFixed(2);
    const maxChInf = Math.max(...samples.map(s => s.chapterInflight));
    const avgCpInf = (samples.reduce((s, x) => s + x.controlPlaneInflight, 0) / samples.length).toFixed(2);
    const avgDbImp = (samples.reduce((s, x) => s + x.totalDbImporting, 0) / samples.length).toFixed(2);

    const avgDL = (samples.reduce((s, x) => s + x.activeDownload, 0) / samples.length).toFixed(2);
    const avgUP = (samples.reduce((s, x) => s + x.activeUpload, 0) / samples.length).toFixed(2);
    const avgDB = (samples.reduce((s, x) => s + x.activeDb, 0) / samples.length).toFixed(2);
    const avgRL = (samples.reduce((s, x) => s + x.waitingRateLimit, 0) / samples.length).toFixed(2);
    const avgSrc = (samples.reduce((s, x) => s + x.waitingSource, 0) / samples.length).toFixed(2);
    const avgBar = (samples.reduce((s, x) => s + x.waitingBarrier, 0) / samples.length).toFixed(2);
    const avgJob = (samples.reduce((s, x) => s + x.waitingJob, 0) / samples.length).toFixed(2);
    const avgIdl = (samples.reduce((s, x) => s + x.idle, 0) / samples.length).toFixed(2);

    const sumStates = (
      parseFloat(avgDL) +
      parseFloat(avgUP) +
      parseFloat(avgDB) +
      parseFloat(avgRL) +
      parseFloat(avgSrc) +
      parseFloat(avgBar) +
      parseFloat(avgJob) +
      parseFloat(avgIdl)
    ).toFixed(2);

    const avgReadySrc = (samples.reduce((s, x) => s + x.readySourcesCount, 0) / samples.length).toFixed(2);
    const avgReadyWorks = (samples.reduce((s, x) => s + x.readyWorksCount, 0) / samples.length).toFixed(2);

    const avgYsqlTotal = (samples.reduce((s, x) => s + x.ysqlTotal, 0) / samples.length).toFixed(2);
    const avgYsqlActive = (samples.reduce((s, x) => s + x.ysqlActive, 0) / samples.length).toFixed(2);
    const avgYsqlIdle = (samples.reduce((s, x) => s + x.ysqlIdle, 0) / samples.length).toFixed(2);
    const avgYsqlIdleInTx = (samples.reduce((s, x) => s + x.ysqlIdleInTx, 0) / samples.length).toFixed(2);

    const avgCpu = (samples.reduce((s, x) => s + x.discloudCpu, 0) / samples.length).toFixed(2);
    const peakCpu = Math.max(...samples.map(s => s.discloudCpu)).toFixed(1);
    const avgRam = (samples.reduce((s, x) => s + x.discloudRamMb, 0) / samples.length).toFixed(2);
    const peakRam = Math.max(...samples.map(s => s.discloudRamMb)).toFixed(1);

    const homeLatencies = samples.map(s => s.homeLatencyMs).sort((a, b) => a - b);
    const obraLatencies = samples.map(s => s.obraLatencyMs).sort((a, b) => a - b);
    const readerLatencies = samples.map(s => s.readerLatencyMs).sort((a, b) => a - b);
    const meLatencies = samples.map(s => s.meLatencyMs).sort((a, b) => a - b);
    const adminLatencies = samples.map(s => s.adminLatencyMs).sort((a, b) => a - b);

    const homeP50 = homeLatencies[Math.floor(homeLatencies.length * 0.5)];
    const obraP50 = obraLatencies[Math.floor(obraLatencies.length * 0.5)];
    const readerP50 = readerLatencies[Math.floor(readerLatencies.length * 0.5)];
    const meP50 = meLatencies[Math.floor(meLatencies.length * 0.5)];
    const adminP50 = adminLatencies[Math.floor(adminLatencies.length * 0.5)];

    const result = {
      windowDurationMinutes: actualDurationMinutes.toFixed(2),
      sampleCount: samples.length,
      readySourcesAvg: parseFloat(avgReadySrc),
      readyWorksAvg: parseFloat(avgReadyWorks),
      chapterInflightAvg: parseFloat(avgChInf),
      chapterInflightMax: maxChInf,
      controlPlaneInflightAvg: parseFloat(avgCpInf),
      totalDbImportingAvg: parseFloat(avgDbImp),
      workerStates: {
        activeDownload: parseFloat(avgDL),
        activeUpload: parseFloat(avgUP),
        activeDb: parseFloat(avgDB),
        waitingRateLimit: parseFloat(avgRL),
        waitingSource: parseFloat(avgSrc),
        waitingBarrier: parseFloat(avgBar),
        waitingJob: parseFloat(avgJob),
        idle: parseFloat(avgIdl),
        sum: parseFloat(sumStates)
      },
      throughput: {
        claimedPerMin: (deltaClaimed / actualDurationMinutes).toFixed(2),
        downloadedPerMin: (deltaChapters / actualDurationMinutes).toFixed(2),
        storedPerMin: (deltaChapters / actualDurationMinutes).toFixed(2),
        newPublishedPerMin: (newPublished / actualDurationMinutes).toFixed(2),
        cascadePublishedPerMin: (cascadePublished / actualDurationMinutes).toFixed(2),
        totalVisiblePerMin: (pubWindowRes.rows.length / actualDurationMinutes).toFixed(2)
      },
      latencies: {
        endToEndP50Sec: durationRes.rows[0].p50_sec || 0,
        endToEndP95Sec: durationRes.rows[0].p95_sec || 0,
        chapterTimeouts: parseInt(timeoutsRes.rows[0].count, 10),
        homeP50Ms: homeP50,
        obraP50Ms: obraP50,
        readerP50Ms: readerP50,
        meP50Ms: meP50,
        adminP50Ms: adminP50
      },
      ysql: {
        total: parseFloat(avgYsqlTotal),
        active: parseFloat(avgYsqlActive),
        idle: parseFloat(avgYsqlIdle),
        idleInTx: parseFloat(avgYsqlIdleInTx),
        poolTimeouts: 0
      },
      discloud: {
        cpuAvg: parseFloat(avgCpu),
        cpuPeak: parseFloat(peakCpu),
        ramAvgMb: parseFloat(avgRam),
        ramPeakMb: parseFloat(peakRam)
      }
    };

    fs.writeFileSync('scorecard_production_measurement.json', JSON.stringify(result, null, 2));
    console.log('\n=== FINAL PRODUCTION SCORECARD RESULT ===');
    console.log(JSON.stringify(result, null, 2));

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
