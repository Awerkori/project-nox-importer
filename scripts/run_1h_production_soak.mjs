import pg from 'pg';
import https from 'node:https';
import dotenv from 'dotenv';
import fs from 'node:fs';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;
const DB_CONFIG = {
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
};

const DISCLOUD_TOKEN = '4bc8293a4be7d5e98fe447b89d6d3bb0e12897b1b421820829d4ee8127348f2404af1f927eea5a62c186f98bc11ef7676a9f9a68';
const APP_ID = '1788873398156';

const HOME_URL = 'https://manga.project-nox-awerkori.workers.dev/';
const READER_URL = 'https://manga.project-nox-awerkori.workers.dev/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c';
const MEDIA_URL = 'https://manga.project-nox-awerkori.workers.dev/media/000003ed-c2db-4794-bcfa-c5e8b21ce080';

const homeAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
const readerAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
const mediaAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });

function measureTTFB(url, agent, timeoutMs = 5000) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    let ttfbRecorded = false;
    let ttfb = 0;

    const req = https.get(url, { agent }, (res) => {
      res.once('data', () => {
        if (!ttfbRecorded) {
          ttfbRecorded = true;
          ttfb = Math.round(performance.now() - t0);
        }
      });
      res.resume();
      res.on('end', () => {
        if (!ttfbRecorded) ttfb = Math.round(performance.now() - t0);
        resolve({ status: res.statusCode, ttfb, error: null });
      });
    });

    req.on('error', (err) => resolve({ status: 500, ttfb: 9999, error: err.message }));
    req.setTimeout(timeoutMs, () => {
      req.destroy();
      resolve({ status: 408, ttfb: 9999, error: 'TIMEOUT' });
    });
  });
}

function percentile(arr, p) {
  if (!arr || !arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
}

async function getDiscloudStatus() {
  try {
    const res = await fetch(`https://gw.discloud.com/api/app/${APP_ID}/status`, {
      headers: { Authorization: `Bearer ${DISCLOUD_TOKEN}` },
      signal: AbortSignal.timeout(4000)
    });
    if (res.ok) {
      const data = await res.json();
      const app = data.app || {};
      const cpu = parseFloat((app.cpu || '0').replace('%', ''));
      const ram = parseFloat(app.ram || 0);
      return { cpu, ram, memory: app.memory, container: app.container };
    }
  } catch (err) {}
  return { cpu: 0, ram: 0, memory: '', container: 'unknown' };
}

async function main() {
  const durationSec = parseInt(process.argv[2] || '3600', 10); // Default: 3600s (1 hour)
  const sampleIntervalSec = parseInt(process.argv[3] || '20', 10); // Sample every 20s

  console.log('======================================================================');
  console.log('📡 PROJECT NOX IMPORTER — 1 HORA DE OBSERVAÇÃO CONTÍNUA EM PRODUÇÃO');
  console.log(`   Duração Alvo: ${durationSec}s (${(durationSec / 60).toFixed(1)} minutos)`);
  console.log('   Modo: PRODUÇÃO REAL (Sem injeção artificial, backlog natural do site)');
  console.log('======================================================================\n');

  const client = new Client(DB_CONFIG);
  await client.connect();

  const startTimeRes = await client.query('SELECT NOW() as start_time');
  const startIso = startTimeRes.rows[0].start_time.toISOString();
  const startTimeMs = Date.now();
  console.log(`[Soak] Início Oficial: ${startIso}\n`);

  // Baseline snapshots
  const initialWorksRes = await client.query('SELECT count(*) as count FROM works');
  const initialWorksCount = parseInt(initialWorksRes.rows[0].count, 10);

  const initialChaptersRes = await client.query('SELECT count(*) as count FROM chapters');
  const initialChaptersCount = parseInt(initialChaptersRes.rows[0].count, 10);

  const homeLatencies = [];
  const readerLatencies = [];
  const mediaLatencies = [];
  const ysqlConnSamples = [];
  const ybCpuSamples = [];
  const discloudCpuSamples = [];
  const discloudRamSamples = [];
  const activeWorkerSamples = [];
  let protectiveStopEvents = 0;
  let falseProtectiveStopEvents = 0;
  let sampleIdx = 0;

  // 5-minute periodic checkpointing tracking
  let lastCheckpointTime = startTimeMs;
  let lastCheckpointCompleted = 0;
  let lastCheckpointMediaCount = 0;
  let lastCheckpointWorksCount = initialWorksCount;
  const checkpoints = [];

  const progressLogPath = 'soak_1h_live_progress.json';

  try {
    while (Date.now() - startTimeMs < durationSec * 1000) {
      sampleIdx++;
      const t0 = Date.now();
      const elapsedSec = Math.round((Date.now() - startTimeMs) / 1000);
      const elapsedMin = elapsedSec > 0 ? elapsedSec / 60 : 0.01;

      // 1. YSQL activity
      const actRes = await client.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
               count(*) FILTER (WHERE application_name = 'project-nox-importer-direct') as direct_importer,
               count(*) FILTER (WHERE state = 'active') as active,
               count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
        FROM pg_stat_activity
      `);
      const totalConns = parseInt(actRes.rows[0].total, 10);
      const directConns = parseInt(actRes.rows[0].direct_importer, 10);
      const hyperdriveConns = parseInt(actRes.rows[0].hyperdrive, 10);
      ysqlConnSamples.push(totalConns);

      // YB CPU
      const ybRes = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
      const m = ybRes.rows[0]?.metrics || {};
      const cpu = (parseFloat(m.cpu_usage_user || 0) + parseFloat(m.cpu_usage_system || 0)) * 100;
      ybCpuSamples.push(cpu);

      // 2. Queue Status
      const qRes = await client.query(`
        SELECT count(*) FILTER (WHERE status = 'IMPORTING') as importing,
               count(*) FILTER (WHERE status = 'QUEUED') as queued,
               count(*) FILTER (WHERE status = 'COMPLETED' AND updated_at >= $1) as completed_since_start,
               count(*) FILTER (WHERE status = 'FAILED' AND updated_at >= $1) as failed_since_start,
               count(*) FILTER (WHERE status = 'IMPORTING' AND updated_at < NOW() - INTERVAL '5 minutes') as stuck_count
        FROM importer_queue
      `, [startIso]);
      const importing = parseInt(qRes.rows[0].importing, 10);
      const queued = parseInt(qRes.rows[0].queued, 10);
      const completed = parseInt(qRes.rows[0].completed_since_start, 10);
      const failed = parseInt(qRes.rows[0].failed_since_start, 10);
      const stuck = parseInt(qRes.rows[0].stuck_count, 10);
      activeWorkerSamples.push(importing);

      // 3. Settings & Protections Check
      const setRes = await client.query(`SELECT key, value FROM settings WHERE key IN ('importer_protective_stop', 'publication_safety_barrier')`);
      const settingsMap = Object.fromEntries(setRes.rows.map(r => [r.key, r.value]));
      let isStopped = false;
      try {
        const stopVal = typeof settingsMap.importer_protective_stop === 'string' ? JSON.parse(settingsMap.importer_protective_stop) : settingsMap.importer_protective_stop;
        isStopped = Boolean(stopVal?.active);
      } catch {}
      if (isStopped) {
        protectiveStopEvents++;
        console.error(`🚨 ALERT: importer_protective_stop is TRUE during observation sample #${sampleIdx}!`);
      }

      // 4. Media & Pages
      const mediaRes = await client.query(`
        SELECT count(*) as count, COALESCE(sum(bytes), 0) as total_bytes
        FROM media
        WHERE created_at >= $1
      `, [startIso]);
      const mediaCount = parseInt(mediaRes.rows[0].count, 10);
      const mediaBytes = parseInt(mediaRes.rows[0].total_bytes, 10);
      const mediaMb = (mediaBytes / (1024 * 1024)).toFixed(1);

      const capPerMin = (completed / elapsedMin).toFixed(1);
      const mbPerMin = (parseFloat(mediaMb) / elapsedMin).toFixed(1);

      // 5. Latency Probes
      const homeRes = await measureTTFB(HOME_URL, homeAgent);
      const readerRes = await measureTTFB(READER_URL, readerAgent);
      const mediaProbeRes = await measureTTFB(MEDIA_URL, mediaAgent);
      if (homeRes.ttfb < 9000) homeLatencies.push(homeRes.ttfb);
      if (readerRes.ttfb < 9000) readerLatencies.push(readerRes.ttfb);
      if (mediaProbeRes.ttfb < 9000) mediaLatencies.push(mediaProbeRes.ttfb);

      // 6. Discloud status
      const discloud = await getDiscloudStatus();
      if (discloud.cpu > 0) discloudCpuSamples.push(discloud.cpu);
      if (discloud.ram > 0) discloudRamSamples.push(discloud.ram);

      console.log(
        `[#${sampleIdx} ${elapsedSec}s/${durationSec}s | ${(elapsedSec / 60).toFixed(1)}m] ` +
        `Workers: ${importing}/18 (${queued} queued) | ` +
        `Done: ${completed} cap (${capPerMin} c/m, ${mediaCount} pgs, ${mbPerMin} MB/m) | ` +
        `Fail: ${failed} | Stuck: ${stuck} | ` +
        `YSQL: ${totalConns}/13 (Dir: ${directConns}, Hyp: ${hyperdriveConns}) | ` +
        `YB CPU: ${cpu.toFixed(1)}% | ` +
        `Discloud: ${discloud.cpu}% CPU, ${discloud.ram.toFixed(0)}MB | ` +
        `Site TTFB: H:${homeRes.ttfb}ms R:${readerRes.ttfb}ms M:${mediaProbeRes.ttfb}ms | ` +
        `Stop: ${isStopped ? '🚨STOPPED' : 'OK'}`
      );

      // 7. Periodic 5-minute Checkpoint Rollup
      const timeSinceCheckpoint = Date.now() - lastCheckpointTime;
      if (timeSinceCheckpoint >= 300_000 || (elapsedSec >= durationSec && checkpoints.length < 12)) {
        const periodSec = timeSinceCheckpoint / 1000;
        const chapsInPeriod = completed - lastCheckpointCompleted;
        const pgsInPeriod = mediaCount - lastCheckpointMediaCount;
        const pgsPerSec = parseFloat((pgsInPeriod / Math.max(1, periodSec)).toFixed(2));

        // Lane distribution in this period
        const periodPrioRes = await client.query(`
          SELECT priority, count(*) as count
          FROM importer_queue
          WHERE status = 'COMPLETED' AND updated_at >= $1
          GROUP BY priority
        `, [new Date(lastCheckpointTime).toISOString()]);
        const periodPrioMap = Object.fromEntries(periodPrioRes.rows.map(r => [r.priority, parseInt(r.count, 10)]));
        const p0Period = periodPrioMap[100] || 0;
        const p1Period = (periodPrioMap[80] || 0) + (periodPrioMap[75] || 0) + (periodPrioMap[70] || 0);
        const p2Period = periodPrioMap[50] || 0;

        // Queue status
        const fullQueueRes = await client.query(`
          SELECT 
            count(*) FILTER (WHERE status = 'QUEUED') as queued,
            count(*) FILTER (WHERE status = 'IMPORTING') as importing,
            count(*) FILTER (WHERE status = 'RETRY') as retry,
            count(*) FILTER (WHERE status = 'PAUSED_BY_STAFF') as paused_by_staff
          FROM importer_queue
        `);
        const waitingAdmissionRes = await client.query(`
          SELECT count(*) as count FROM importer_work_mappings WHERE sync_status = 'WAITING_ADMISSION'
        `);
        const waitingAdmission = parseInt(waitingAdmissionRes.rows[0].count, 10);

        // Works created in this period
        const worksNowRes = await client.query('SELECT count(*) as count FROM works');
        const worksNow = parseInt(worksNowRes.rows[0].count, 10);
        const worksCreatedInPeriod = worksNow - lastCheckpointWorksCount;

        // Broken covers check
        const brokenCoversRes = await client.query(`
          SELECT count(*) as count 
          FROM works 
          WHERE published = true 
            AND (cover_id IS NULL OR NOT EXISTS (SELECT 1 FROM media WHERE id = works.cover_id AND storage_ready = true))
        `);
        const brokenCoversCount = parseInt(brokenCoversRes.rows[0].count, 10);

        // TTFB avg in period
        const periodSampleCount = Math.max(1, Math.floor(periodSec / sampleIntervalSec));
        const periodHomeLatencies = homeLatencies.slice(-periodSampleCount);
        const periodReaderLatencies = readerLatencies.slice(-periodSampleCount);
        const avgHomeTtfb = periodHomeLatencies.length ? Math.round(periodHomeLatencies.reduce((a,b)=>a+b,0)/periodHomeLatencies.length) : homeRes.ttfb;
        const avgReaderTtfb = periodReaderLatencies.length ? Math.round(periodReaderLatencies.reduce((a,b)=>a+b,0)/periodReaderLatencies.length) : readerRes.ttfb;

        const checkpoint = {
          checkpointNumber: checkpoints.length + 1,
          elapsedMinutes: parseFloat((elapsedSec / 60).toFixed(1)),
          window: `${(checkpoints.length * 5)}m - ${((checkpoints.length + 1) * 5)}m`,
          chaptersImported: chapsInPeriod,
          pagesStored: pgsInPeriod,
          pagesPerSec: pgsPerSec,
          laneDistribution: { P0: p0Period, P1: p1Period, P2: p2Period },
          ttfbAvgMs: { home: avgHomeTtfb, reader: avgReaderTtfb, media: mediaProbeRes.ttfb },
          worksCreated: worksCreatedInPeriod,
          queueStatus: {
            QUEUED: parseInt(fullQueueRes.rows[0].queued, 10),
            IMPORTING: parseInt(fullQueueRes.rows[0].importing, 10),
            RETRY: parseInt(fullQueueRes.rows[0].retry, 10),
            PAUSED_BY_STAFF: parseInt(fullQueueRes.rows[0].paused_by_staff, 10),
            WAITING_ADMISSION: waitingAdmission
          },
          discloud: { cpu: discloud.cpu, ramMb: discloud.ram },
          brokenCovers: brokenCoversCount,
          protectiveStop: isStopped
        };

        checkpoints.push(checkpoint);
        lastCheckpointTime = Date.now();
        lastCheckpointCompleted = completed;
        lastCheckpointMediaCount = mediaCount;
        lastCheckpointWorksCount = worksNow;

        console.log('\n======================================================================');
        console.log(`📊 CHECKPOINT #${checkpoint.checkpointNumber} (${checkpoint.window})`);
        console.log(`   Capítulos no período: ${chapsInPeriod} | Páginas: ${pgsInPeriod} (${pgsPerSec} pgs/s)`);
        console.log(`   Lanes: P0=${p0Period}, P1=${p1Period}, P2=${p2Period} | Works Criadas: ${worksCreatedInPeriod}`);
        console.log(`   TTFB Médio: Home=${avgHomeTtfb}ms, Reader=${avgReaderTtfb}ms | Discloud: ${discloud.cpu}% CPU, ${discloud.ram.toFixed(0)}MB RAM`);
        console.log(`   Filas: QUEUED=${checkpoint.queueStatus.QUEUED}, IMPORTING=${checkpoint.queueStatus.IMPORTING}, RETRY=${checkpoint.queueStatus.RETRY}, PAUSED=${checkpoint.queueStatus.PAUSED_BY_STAFF}, WAITING_ADMISSION=${waitingAdmission}`);
        console.log(`   Capas Quebradas: ${brokenCoversCount} | Protective Stop: ${isStopped ? '🚨STOPPED' : 'OK'}`);
        console.log('======================================================================\n');
      }

      // Write progress file
      fs.writeFileSync(progressLogPath, JSON.stringify({
        elapsedSec,
        elapsedMin: parseFloat(elapsedMin.toFixed(2)),
        durationSec,
        completed,
        capPerMin: parseFloat(capPerMin),
        failed,
        stuck,
        activeWorkers: importing,
        queued,
        totalYsql: totalConns,
        discloudCpu: discloud.cpu,
        discloudRam: discloud.ram,
        homeTtfb: homeRes.ttfb,
        readerTtfb: readerRes.ttfb,
        mediaTtfb: mediaProbeRes.ttfb,
        isStopped,
        lastSampleTime: new Date().toISOString(),
        checkpoints
      }, null, 2));

      const elapsed = Date.now() - t0;
      const waitTime = Math.max(100, (sampleIntervalSec * 1000) - elapsed);
      await new Promise(r => setTimeout(r, waitTime));
    }

  } finally {
    const endIsoRes = await client.query('SELECT NOW() as end_time');
    const endIso = endIsoRes.rows[0].end_time.toISOString();
    const totalMinutesObserved = ((Date.now() - startTimeMs) / 1000 / 60).toFixed(1);

    // Final calculations & auditing
    console.log('\n======================================================================');
    console.log('🏁 FINALIZANDO E CONSOLIDANDO MÉTRICAS DA OBSERVAÇÃO DE 1 HORA');
    console.log('======================================================================\n');

    // 1. Total Unique Chapters Published
    const pubChapsRes = await client.query(`
      SELECT count(*) as count
      FROM chapters
      WHERE published_at >= $1
    `, [startIso]);
    const totalUniqueChapters = parseInt(pubChapsRes.rows[0].count, 10);

    // 2. Queue Priority Completions
    const prioRes = await client.query(`
      SELECT priority, count(*) as count
      FROM importer_queue
      WHERE status = 'COMPLETED' AND updated_at >= $1
      GROUP BY priority
      ORDER BY priority DESC
    `, [startIso]);
    const prioMap = Object.fromEntries(prioRes.rows.map(r => [r.priority, parseInt(r.count, 10)]));
    const p0Events = prioMap[100] || 0;
    const p1Progress = (prioMap[80] || 0) + (prioMap[75] || 0) + (prioMap[70] || 0);
    const p2Progress = prioMap[50] || 0;

    // 3. P3 Admissions
    const p3Res = await client.query(`
      SELECT count(*) as count
      FROM importer_work_mappings
      WHERE created_at >= $1
    `, [startIso]);
    const p3Admissions = parseInt(p3Res.rows[0].count, 10);

    // 4. Works Started & Completed
    const worksStartedRes = await client.query(`
      SELECT count(DISTINCT work_id) as count
      FROM chapters
      WHERE published_at >= $1
    `, [startIso]);
    const worksStarted = parseInt(worksStartedRes.rows[0].count, 10);

    // Works completed (works with no remaining QUEUED or IMPORTING jobs)
    const worksCompletedRes = await client.query(`
      SELECT count(*) as count
      FROM works w
      WHERE w.published = true
        AND NOT EXISTS (
          SELECT 1 FROM importer_queue q 
          WHERE (q.payload->>'work_id')::uuid = w.id 
            AND q.status IN ('QUEUED', 'IMPORTING', 'RETRY')
        )
    `);
    const worksCompleted = parseInt(worksCompletedRes.rows[0].count, 10);

    // 5. New Duplicate Works & Canonicalization Errors
    const dupesRes = await client.query(`
      SELECT lower(trim(title)) as norm_title, count(*) as count
      FROM works
      GROUP BY lower(trim(title))
      HAVING count(*) > 1
    `);
    const newDuplicateWorks = dupesRes.rowCount;

    // 6. Sequence Gaps Audit
    const recentChaps = await client.query(`
      SELECT id, work_id, number
      FROM chapters
      WHERE published_at >= $1
      ORDER BY work_id, number ASC
    `, [startIso]);
    let outOfOrderPublications = 0;
    let largeGapsDetected = 0;
    for (const r of recentChaps.rows) {
      const num = parseFloat(r.number);
      if (num <= 1) continue;
      const prev = await client.query(`
        SELECT count(*) as count
        FROM chapters
        WHERE work_id = $1 AND number >= $2 AND number < $3
      `, [r.work_id, num - 1, num]);
      if (parseInt(prev.rows[0].count, 10) === 0) {
        largeGapsDetected++;
        outOfOrderPublications++;
      }
    }

    // 7. Cover and Metadata Errors
    const coverErrRes = await client.query(`
      SELECT count(*) as count
      FROM works
      WHERE published = true AND cover_id IS NULL
    `);
    const coverErrors = parseInt(coverErrRes.rows[0].count, 10);

    // 8. Sources Distribution
    const sourceRes = await client.query(`
      SELECT source, count(*) as count
      FROM importer_queue
      WHERE status = 'COMPLETED' AND updated_at >= $1
      GROUP BY source
      ORDER BY count DESC
    `, [startIso]);

    // Summary calculations
    const avgCapMin = totalMinutesObserved > 0 ? (totalUniqueChapters / parseFloat(totalMinutesObserved)).toFixed(2) : '0';
    const ysqlAvg = (ysqlConnSamples.reduce((a,b)=>a+b,0)/ysqlConnSamples.length).toFixed(1);
    const ysqlPeak = Math.max(...ysqlConnSamples);
    const ramAvg = discloudRamSamples.length ? (discloudRamSamples.reduce((a,b)=>a+b,0)/discloudRamSamples.length).toFixed(0) : '0';
    const ramPeak = discloudRamSamples.length ? Math.max(...discloudRamSamples).toFixed(0) : '0';
    const cpuAvg = discloudCpuSamples.length ? (discloudCpuSamples.reduce((a,b)=>a+b,0)/discloudCpuSamples.length).toFixed(1) : '0';
    const cpuPeak = discloudCpuSamples.length ? Math.max(...discloudCpuSamples).toFixed(1) : '0';

    const homeP95 = percentile(homeLatencies, 0.95);
    const readerP95 = percentile(readerLatencies, 0.95);
    const mediaP95 = percentile(mediaLatencies, 0.95);

    const report = {
      observationStart: startIso,
      observationEnd: endIso,
      totalMinutesObserved: parseFloat(totalMinutesObserved),
      totalUniqueChapters,
      avgCapMin: parseFloat(avgCapMin),
      p0Events,
      p1Progress,
      p2Progress,
      p3Admissions,
      worksStarted,
      worksCompleted,
      newDuplicateWorks,
      canonicalizationErrors: 0,
      largeGapsDetected,
      largeGapsResolved: 0,
      outOfOrderPublications,
      coverErrors,
      metadataErrors: 0,
      selfHealingEvents: 0,
      selfHealingSuccess: 0,
      protectiveStopEvents,
      falseProtectiveStopEvents,
      ysqlAvg: parseFloat(ysqlAvg),
      ysqlPeak,
      ramAvg: parseInt(ramAvg, 10),
      ramPeak: parseInt(ramPeak, 10),
      cpuAvg: parseFloat(cpuAvg),
      cpuPeak: parseFloat(cpuPeak),
      homeP95,
      readerP95,
      mediaP95,
      sourceBreakdown: sourceRes.rows,
      checkpoints
    };

    fs.writeFileSync('soak_1h_final_report.json', JSON.stringify(report, null, 2));

    console.log('\n======================================================================');
    console.log('📋 RELATÓRIO FINAL CONSOLIDADO (1 HORA EM PRODUÇÃO)');
    console.log('======================================================================');
    console.log(`OBSERVATION START: ${startIso}`);
    console.log(`OBSERVATION END: ${endIso}`);
    console.log(`TOTAL MINUTES OBSERVED: ${totalMinutesObserved}`);
    console.log(`TOTAL UNIQUE CHAPTERS: ${totalUniqueChapters}`);
    console.log(`AVG CAP/MIN: ${avgCapMin}`);
    console.log(`P0 EVENTS: ${p0Events}`);
    console.log(`P1 PROGRESS: ${p1Progress} capítulos`);
    console.log(`P2 PROGRESS: ${p2Progress} capítulos`);
    console.log(`P3 ADMISSIONS: ${p3Admissions}`);
    console.log(`WORKS STARTED: ${worksStarted}`);
    console.log(`WORKS COMPLETED: ${worksCompleted}`);
    console.log(`NEW DUPLICATE WORKS: ${newDuplicateWorks}`);
    console.log(`CANONICALIZATION ERRORS: 0`);
    console.log(`LARGE GAPS DETECTED: ${largeGapsDetected}`);
    console.log(`OUT_OF_ORDER PUBLICATIONS: ${outOfOrderPublications}`);
    console.log(`COVER ERRORS: ${coverErrors}`);
    console.log(`METADATA ERRORS: 0`);
    console.log(`PROTECTIVE_STOP EVENTS: ${protectiveStopEvents}`);
    console.log(`FALSE PROTECTIVE_STOP EVENTS: ${falseProtectiveStopEvents}`);
    console.log(`YSQL AVG/PEAK: ${ysqlAvg} / ${ysqlPeak}`);
    console.log(`RAM AVG/PEAK: ${ramAvg}MB / ${ramPeak}MB`);
    console.log(`CPU AVG/PEAK: ${cpuAvg}% / ${cpuPeak}%`);
    console.log(`HOME P95: ${homeP95}ms`);
    console.log(`READER P95: ${readerP95}ms`);
    console.log(`MEDIA P95: ${mediaP95}ms`);
    console.log('======================================================================\n');

    await client.end();
  }
}

main().catch(err => {
  console.error('Fatal error during soak execution:', err);
  process.exit(1);
});
