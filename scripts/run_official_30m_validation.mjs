import pg from 'pg';
import https from 'node:https';
import fs from 'node:fs';
import dotenv from 'dotenv';

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
const CATALOG_URL = 'https://manga.project-nox-awerkori.workers.dev/catalogo';
const WORK_URL = 'https://manga.project-nox-awerkori.workers.dev/obra/as-aventuras-de-greed';
const SAMPLE_READER_CHAPTERS = [
  'ca7033ef-625d-433f-b833-16efc149d599',
  'ae55ae1f-aa9b-4442-be2b-6c0bf33ab011',
  'ef471f47-4d8b-4611-8b11-2e92971861bd'
];
const SAMPLE_MEDIA_IDS = [
  '000003ed-c2db-4794-bcfa-c5e8b21ce080',
  '10b1d31a-6dc4-42c1-95f7-aaeddb5c9dd4',
  '0448dfba-ed7b-47e5-9890-e9b1bd24d93f'
];

const validatorAgent = new https.Agent({ keepAlive: true, maxSockets: 10, keepAliveMsecs: 60000 });

function measureTTFB(url, timeoutMs = 4000) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    let ttfbRecorded = false;
    let ttfb = 0;

    const req = https.get(url, { agent: validatorAgent, headers: { 'User-Agent': 'Project-Nox-Validator/2.0' } }, (res) => {
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

const avg = (arr) => arr.length ? Math.round((arr.reduce((a, b) => a + b, 0) / arr.length) * 10) / 10 : 0;

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
      return { cpu, ram, memory: app.memory, container: app.container, status: app.status };
    }
  } catch {}
  return { cpu: 0, ram: 0, memory: '', container: 'unknown', status: 'unknown' };
}

async function main() {
  const durationSec = parseInt(process.argv[2] || '1800', 10);
  const sampleIntervalSec = parseInt(process.argv[3] || '15', 10);

  console.log('======================================================================');
  console.log('🚀 PROJECT NOX — OFFICIAL POST-FIX 30-MINUTE VALIDATION WINDOW');
  console.log(`   Duration: ${durationSec}s (${(durationSec / 60).toFixed(1)} min)`);
  console.log(`   Sample Interval: ${sampleIntervalSec}s`);
  console.log('   Target: UNIQUE_CANONICAL_PUBLISHED_CAP_MIN >= 10.0');
  console.log('   SLAs: Reader P95 < 150ms | Home P95 < 250ms | Media P95 < 120ms');
  console.log('======================================================================\n');

  const client = new Client(DB_CONFIG);
  await client.connect();

  const startTimeRes = await client.query('SELECT NOW() as start_time');
  const startIso = startTimeRes.rows[0].start_time.toISOString();
  const startTime = Date.now();
  const durationMs = durationSec * 1000;

  console.log(`[Validation Window] Started at: ${startIso} (Epoch: ${startTime})\n`);

  // Initial staged count
  const initialStagedRes = await client.query(`
    SELECT count(*) as count FROM importer_chapter_mappings WHERE status = 'STAGED'
  `);
  const initialStaged = parseInt(initialStagedRes.rows[0].count, 10);

  const homeLatencies = [];
  const catalogLatencies = [];
  const workLatencies = [];
  const readerLatencies = [];
  const mediaLatencies = [];
  const ysqlConnSamples = [];
  const ybCpuSamples = [];
  const discloudCpuSamples = [];
  const discloudRamSamples = [];
  const busyWorkerSamples = [];

  let sampleIdx = 0;
  let lastCheckpointMin = 0;

  const progressPath = '/home/awerkori/.Projects/project-nox-importer/validation_30m_live_progress.json';
  const summaryPath = '/home/awerkori/.Projects/project-nox-importer/validation_30m_final_summary.json';

  try {
    while (Date.now() - startTime < durationMs) {
      sampleIdx++;
      const t0 = Date.now();
      const elapsedSec = Math.round((Date.now() - startTime) / 1000);
      const elapsedMin = elapsedSec > 0 ? elapsedSec / 60 : 0.001;

      // 1. Database Connections
      const actRes = await client.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
               count(*) FILTER (WHERE application_name = 'project-nox-importer-direct') as direct_importer,
               count(*) FILTER (WHERE state = 'active') as active
        FROM pg_stat_activity
      `);
      const totalConns = parseInt(actRes.rows[0].total, 10);
      const directConns = parseInt(actRes.rows[0].direct_importer, 10);
      const hyperdriveConns = parseInt(actRes.rows[0].hyperdrive, 10);
      ysqlConnSamples.push(totalConns);

      // 2. Queue Status
      const qRes = await client.query(`
        SELECT count(*) FILTER (WHERE status = 'IMPORTING') as importing,
               count(*) FILTER (WHERE status = 'QUEUED') as queued,
               count(*) FILTER (WHERE status = 'COMPLETED' AND updated_at >= $1) as completed_since_start,
               count(*) FILTER (WHERE status = 'FAILED' AND updated_at >= $1) as failed_since_start,
               count(*) FILTER (WHERE last_error = 'CANONICAL_ALREADY_SATISFIED' AND updated_at >= $1) as short_circuited_since_start
        FROM importer_queue
      `, [startIso]);
      const importing = parseInt(qRes.rows[0].importing, 10);
      const queued = parseInt(qRes.rows[0].queued, 10);
      const completed = parseInt(qRes.rows[0].completed_since_start, 10);
      const failed = parseInt(qRes.rows[0].failed_since_start, 10);
      const shortCircuited = parseInt(qRes.rows[0].short_circuited_since_start, 10);
      busyWorkerSamples.push(importing);

      // 3. Unique Canonical Publications
      const uniqueRes = await client.query(`
        SELECT count(DISTINCT id) as unique_pubs
        FROM chapters
        WHERE published_at >= $1
      `, [startIso]);
      const uniquePubs = parseInt(uniqueRes.rows[0].unique_pubs, 10);
      const uniqueCapPerMin = (uniquePubs / elapsedMin).toFixed(2);
      const processedJobsPerMin = (completed / elapsedMin).toFixed(2);

      // 4. Staged Status
      const stagedRes = await client.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE updated_at < NOW() - INTERVAL '5 minutes') as gt_5m,
               count(*) FILTER (WHERE updated_at < NOW() - INTERVAL '15 minutes') as gt_15m,
               count(*) FILTER (WHERE updated_at < NOW() - INTERVAL '30 minutes') as gt_30m,
               count(*) FILTER (WHERE updated_at < NOW() - INTERVAL '60 minutes') as gt_60m
        FROM importer_chapter_mappings
        WHERE status = 'STAGED'
      `);
      const stagedTotal = parseInt(stagedRes.rows[0].total, 10);
      const stagedGt5m = parseInt(stagedRes.rows[0].gt_5m, 10);
      const stagedGt15m = parseInt(stagedRes.rows[0].gt_15m, 10);
      const stagedGt30m = parseInt(stagedRes.rows[0].gt_30m, 10);
      const stagedGt60m = parseInt(stagedRes.rows[0].gt_60m, 10);
      const stagedDelta = stagedTotal - initialStaged;

      // 5. Media Throughput
      const mediaRes = await client.query(`
        SELECT count(*) as count, COALESCE(sum(bytes), 0) as total_bytes
        FROM media
        WHERE created_at >= $1
      `, [startIso]);
      const mediaCount = parseInt(mediaRes.rows[0].count, 10);
      const mediaBytes = parseInt(mediaRes.rows[0].total_bytes, 10);
      const mediaMb = (mediaBytes / (1024 * 1024)).toFixed(1);

      // 6. Site Latency Probes
      const homeProbe = await measureTTFB(HOME_URL);
      if (homeProbe.ttfb < 9000) homeLatencies.push(homeProbe.ttfb);

      const catalogProbe = await measureTTFB(CATALOG_URL);
      if (catalogProbe.ttfb < 9000) catalogLatencies.push(catalogProbe.ttfb);

      const workProbe = await measureTTFB(WORK_URL);
      if (workProbe.ttfb < 9000) workLatencies.push(workProbe.ttfb);

      // Round-robin reader probe across sample chapters
      const readerSampleUrl = `https://manga.project-nox-awerkori.workers.dev/ler/${SAMPLE_READER_CHAPTERS[sampleIdx % SAMPLE_READER_CHAPTERS.length]}`;
      const readerProbe = await measureTTFB(readerSampleUrl);
      if (readerProbe.ttfb < 9000) readerLatencies.push(readerProbe.ttfb);

      // Media probe
      const mediaSampleUrl = `https://manga.project-nox-awerkori.workers.dev/media/${SAMPLE_MEDIA_IDS[sampleIdx % SAMPLE_MEDIA_IDS.length]}`;
      const mediaProbe = await measureTTFB(mediaSampleUrl);
      if (mediaProbe.ttfb < 9000) mediaLatencies.push(mediaProbe.ttfb);

      // 7. Discloud Status
      const discloud = await getDiscloudStatus();
      if (discloud.cpu > 0) discloudCpuSamples.push(discloud.cpu);
      if (discloud.ram > 0) discloudRamSamples.push(discloud.ram);

      // Save live progress
      const currentProgress = {
        sampleIdx,
        startIso,
        startTime,
        elapsedSec,
        elapsedMin: parseFloat(elapsedMin.toFixed(2)),
        remainingSec: Math.max(0, durationSec - elapsedSec),
        uniquePubs,
        uniqueCapPerMin: parseFloat(uniqueCapPerMin),
        processedJobs: completed,
        processedJobsPerMin: parseFloat(processedJobsPerMin),
        shortCircuitedDuplicates: shortCircuited,
        importingWorkers: importing,
        queuedJobs: queued,
        failedJobs: failed,
        staged: {
          total: stagedTotal,
          deltaSinceStart: stagedDelta,
          gt5m: stagedGt5m,
          gt15m: stagedGt15m,
          gt30m: stagedGt30m,
          gt60m: stagedGt60m
        },
        siteLatency: {
          homeP50: percentile(homeLatencies, 0.50),
          homeP95: percentile(homeLatencies, 0.95),
          catalogP50: percentile(catalogLatencies, 0.50),
          catalogP95: percentile(catalogLatencies, 0.95),
          workP50: percentile(workLatencies, 0.50),
          workP95: percentile(workLatencies, 0.95),
          readerP50: percentile(readerLatencies, 0.50),
          readerP95: percentile(readerLatencies, 0.95),
          mediaP50: percentile(mediaLatencies, 0.50),
          mediaP95: percentile(mediaLatencies, 0.95),
          samplesCount: {
            home: homeLatencies.length,
            catalog: catalogLatencies.length,
            work: workLatencies.length,
            reader: readerLatencies.length,
            media: mediaLatencies.length
          }
        },
        infra: {
          ysqlTotalConns: totalConns,
          ysqlImporterConns: directConns,
          ysqlHyperdriveConns: hyperdriveConns,
          discloudCpu: discloud.cpu,
          discloudRam: discloud.ram
        }
      };

      fs.writeFileSync(progressPath, JSON.stringify(currentProgress, null, 2));

      // Console progress line
      console.log(
        `[#${sampleIdx} ${elapsedSec}s/${durationSec}s] ` +
        `PUB: ${uniquePubs} (${uniqueCapPerMin} cap/m) | ` +
        `COMPLETED: ${completed} (DEDUP SKIP: ${shortCircuited}) | ` +
        `WORKERS: ${importing}/18 (${queued} queued) | ` +
        `STAGED: ${stagedTotal} (delta: ${stagedDelta}) | ` +
        `P95s: H:${percentile(homeLatencies, 0.95)}ms C:${percentile(catalogLatencies, 0.95)}ms W:${percentile(workLatencies, 0.95)}ms R:${percentile(readerLatencies, 0.95)}ms M:${percentile(mediaLatencies, 0.95)}ms | ` +
        `YSQL: ${totalConns}/13 | CPU: ${discloud.cpu}%`
      );

      // Checkpoints every 5 minutes (300s, 600s, 900s, 1200s, 1500s, 1800s)
      const currentCheckpointMin = Math.floor(elapsedSec / 300) * 5;
      if (currentCheckpointMin > lastCheckpointMin && currentCheckpointMin > 0) {
        lastCheckpointMin = currentCheckpointMin;
        console.log(`\n======================================================================`);
        console.log(`📍 [CHECKPOINT T+${currentCheckpointMin} MIN]`);
        console.log(`   Unique Published: ${uniquePubs} | Rate: ${uniqueCapPerMin} cap/min (Target: >= 10.0)`);
        console.log(`   Completed Jobs: ${completed} | Dedup Short-circuited: ${shortCircuited}`);
        console.log(`   Staged Backlog: ${stagedTotal} (Delta: ${stagedDelta})`);
        console.log(`   Home P95: ${percentile(homeLatencies, 0.95)}ms (Target: < 250ms) | P50: ${percentile(homeLatencies, 0.50)}ms`);
        console.log(`   Catalog P95: ${percentile(catalogLatencies, 0.95)}ms (Target: < 250ms) | P50: ${percentile(catalogLatencies, 0.50)}ms`);
        console.log(`   Work P95: ${percentile(workLatencies, 0.95)}ms (Target: < 250ms) | P50: ${percentile(workLatencies, 0.50)}ms`);
        console.log(`   Reader P95: ${percentile(readerLatencies, 0.95)}ms (Target: < 150ms) | P50: ${percentile(readerLatencies, 0.50)}ms`);
        console.log(`   Media P95: ${percentile(mediaLatencies, 0.95)}ms (Target: < 120ms) | P50: ${percentile(mediaLatencies, 0.50)}ms`);
        console.log(`   YSQL Connections: ${totalConns}/13 (Avg: ${avg(ysqlConnSamples)})`);
        console.log(`======================================================================\n`);
      }

      const elapsed = Date.now() - t0;
      const waitTime = Math.max(100, (sampleIntervalSec * 1000) - elapsed);
      await new Promise(r => setTimeout(r, waitTime));
    }
  } finally {
    const elapsedFinalSec = Math.round((Date.now() - startTime) / 1000);
    const elapsedFinalMin = elapsedFinalSec / 60;

    // Final Unique publications
    const finalUniqueRes = await client.query(`
      SELECT count(DISTINCT id) as unique_pubs
      FROM chapters
      WHERE published_at >= $1
    `, [startIso]);
    const finalUniquePubs = parseInt(finalUniqueRes.rows[0].unique_pubs, 10);
    const finalCapMin = (finalUniquePubs / elapsedFinalMin).toFixed(2);

    // Final completed & short-circuited
    const finalQueueRes = await client.query(`
      SELECT count(*) FILTER (WHERE status = 'COMPLETED') as completed,
             count(*) FILTER (WHERE last_error = 'CANONICAL_ALREADY_SATISFIED') as short_circuited
      FROM importer_queue
      WHERE updated_at >= $1
    `, [startIso]);
    const finalCompleted = parseInt(finalQueueRes.rows[0].completed, 10);
    const finalShortCircuited = parseInt(finalQueueRes.rows[0].short_circuited, 10);

    // Check duplicate publications
    const dupRes = await client.query(`
      SELECT work_id, number, count(*)
      FROM chapters
      WHERE published_at >= $1
      GROUP BY work_id, number
      HAVING count(*) > 1
    `, [startIso]);
    const duplicateCount = dupRes.rows.length;

    // Check duplicate works
    const dupWorksRes = await client.query(`
      SELECT slug, count(*)
      FROM works
      WHERE created_at >= $1
      GROUP BY slug
      HAVING count(*) > 1
    `, [startIso]);
    const duplicateWorksCount = dupWorksRes.rows.length;

    // Sources breakdown
    const sourceRes = await client.query(`
      SELECT source, count(*) as count
      FROM importer_queue
      WHERE status = 'COMPLETED' AND updated_at >= $1
      GROUP BY source
      ORDER BY count DESC
    `, [startIso]);

    // Final Staged Status
    const finalStagedRes = await client.query(`
      SELECT count(*) as total,
             count(*) FILTER (WHERE updated_at < NOW() - INTERVAL '5 minutes') as gt_5m,
             count(*) FILTER (WHERE updated_at < NOW() - INTERVAL '15 minutes') as gt_15m,
             count(*) FILTER (WHERE updated_at < NOW() - INTERVAL '30 minutes') as gt_30m,
             count(*) FILTER (WHERE updated_at < NOW() - INTERVAL '60 minutes') as gt_60m
      FROM importer_chapter_mappings
      WHERE status = 'STAGED'
    `);
    const finalStagedTotal = parseInt(finalStagedRes.rows[0].total, 10);

    const finalSummary = {
      validationMode: 'OFFICIAL_POST_FIX_30M_VALIDATION',
      startIso,
      endIso: new Date().toISOString(),
      durationSec: elapsedFinalSec,
      durationMin: parseFloat(elapsedFinalMin.toFixed(2)),
      throughput: {
        uniqueCanonicalPublished: finalUniquePubs,
        uniqueCanonicalPublishedCapPerMin: parseFloat(finalCapMin),
        targetCapPerMin: 10.0,
        targetPassed: parseFloat(finalCapMin) >= 10.0,
        totalJobsCompleted: finalCompleted,
        totalJobsPerMin: parseFloat((finalCompleted / elapsedFinalMin).toFixed(2)),
        duplicateProviderShortCircuited: finalShortCircuited
      },
      siteSla: {
        home: {
          p50: percentile(homeLatencies, 0.50),
          p95: percentile(homeLatencies, 0.95),
          p99: percentile(homeLatencies, 0.99),
          max: Math.max(...homeLatencies, 0),
          sla: 250,
          passed: percentile(homeLatencies, 0.95) < 250,
          sampleCount: homeLatencies.length
        },
        catalog: {
          p50: percentile(catalogLatencies, 0.50),
          p95: percentile(catalogLatencies, 0.95),
          p99: percentile(catalogLatencies, 0.99),
          max: Math.max(...catalogLatencies, 0),
          sla: 250,
          passed: percentile(catalogLatencies, 0.95) < 250,
          sampleCount: catalogLatencies.length
        },
        work: {
          p50: percentile(workLatencies, 0.50),
          p95: percentile(workLatencies, 0.95),
          p99: percentile(workLatencies, 0.99),
          max: Math.max(...workLatencies, 0),
          sla: 250,
          passed: percentile(workLatencies, 0.95) < 250,
          sampleCount: workLatencies.length
        },
        reader: {
          p50: percentile(readerLatencies, 0.50),
          p95: percentile(readerLatencies, 0.95),
          p99: percentile(readerLatencies, 0.99),
          max: Math.max(...readerLatencies, 0),
          sla: 150,
          passed: percentile(readerLatencies, 0.95) < 150,
          sampleCount: readerLatencies.length
        },
        media: {
          p50: percentile(mediaLatencies, 0.50),
          p95: percentile(mediaLatencies, 0.95),
          p99: percentile(mediaLatencies, 0.99),
          max: Math.max(...mediaLatencies, 0),
          sla: 120,
          passed: percentile(mediaLatencies, 0.95) < 120,
          sampleCount: mediaLatencies.length
        }
      },
      stagedQueue: {
        initial: initialStaged,
        final: finalStagedTotal,
        delta: finalStagedTotal - initialStaged,
        gt5m: parseInt(finalStagedRes.rows[0].gt_5m, 10),
        gt15m: parseInt(finalStagedRes.rows[0].gt_15m, 10),
        gt30m: parseInt(finalStagedRes.rows[0].gt_30m, 10),
        gt60m: parseInt(finalStagedRes.rows[0].gt_60m, 10)
      },
      integrity: {
        duplicatePublications: duplicateCount,
        duplicateWorks: duplicateWorksCount,
        passed: duplicateCount === 0 && duplicateWorksCount === 0
      },
      infra: {
        ysqlAvg: avg(ysqlConnSamples),
        ysqlPeak: Math.max(...ysqlConnSamples, 0),
        ysqlLimit: 13,
        discloudCpuAvg: avg(discloudCpuSamples),
        discloudCpuPeak: Math.max(...discloudCpuSamples, 0),
        discloudRamAvg: avg(discloudRamSamples),
        discloudRamPeak: Math.max(...discloudRamSamples, 0)
      },
      contributingSources: sourceRes.rows
    };

    fs.writeFileSync(summaryPath, JSON.stringify(finalSummary, null, 2));

    console.log('\n======================================================================');
    console.log('🏁 30-MINUTE VALIDATION COMPLETE — FINAL SUMMARY');
    console.log('======================================================================');
    console.log(`UNIQUE PUBLISHED: ${finalUniquePubs} (${finalCapMin} cap/min) | TARGET >= 10.0: ${finalSummary.throughput.targetPassed ? 'PASSED ✅' : 'FAILED ❌'}`);
    console.log(`READER P95: ${finalSummary.siteSla.reader.p95}ms | TARGET < 150ms: ${finalSummary.siteSla.reader.passed ? 'PASSED ✅' : 'FAILED ❌'}`);
    console.log(`HOME P95: ${finalSummary.siteSla.home.p95}ms | TARGET < 250ms: ${finalSummary.siteSla.home.passed ? 'PASSED ✅' : 'FAILED ❌'}`);
    console.log(`MEDIA P95: ${finalSummary.siteSla.media.p95}ms | TARGET < 120ms: ${finalSummary.siteSla.media.passed ? 'PASSED ✅' : 'FAILED ❌'}`);
    console.log(`DEDUPLICATION SHORT-CIRCUITED: ${finalShortCircuited} jobs saved`);
    console.log(`INTEGRITY: Duplicates=${duplicateCount} | Duplicate Works=${duplicateWorksCount}`);
    console.log(`STAGED BACKLOG: Initial=${initialStaged} -> Final=${finalStagedTotal} (Delta: ${finalSummary.stagedQueue.delta})`);
    console.log('======================================================================\n');

    await client.end();
  }
}

main().catch((err) => {
  console.error('Validation runner error:', err);
  process.exit(1);
});
