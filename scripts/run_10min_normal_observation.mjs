import pg from 'pg';
import puppeteer from 'puppeteer-core';
import dotenv from 'dotenv';
import fs from 'fs';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const ybClient = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

const PUPPETEER_OPTS = {
  executablePath: '/usr/bin/chromium',
  headless: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
};

const DISCLOUD_COOKIE = {
  name: 'session_id',
  value: '4bc8293a4be7d5e98fe447b89d6d3bb0e12897b1b421820829d4ee8127348f2404af1f927eea5a62c186f98bc11ef7676a9f9a68',
  domain: '.discloud.com',
  path: '/'
};

async function main() {
  await ybClient.connect();
  console.log('Connected to YugabyteDB Aeon cluster.');

  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  const page = await browser.newPage();
  await page.setCookie(DISCLOUD_COOKIE);

  const T0 = new Date();
  const DURATION_SEC = 600; // 10 minutes
  const INTERVAL_SEC = 30;  // Sample every 30s
  const END_TIME = new Date(T0.getTime() + DURATION_SEC * 1000);

  console.log(`\n============================================================`);
  console.log(`STARTING 10-MINUTE NORMAL OPERATION OBSERVATION ON DISCLOUD`);
  console.log(`T0: ${T0.toISOString()}`);
  console.log(`Planned End: ${END_TIME.toISOString()} (${DURATION_SEC}s)`);
  console.log(`No synthetic load injected — pure production traffic & pipeline.`);
  console.log(`============================================================\n`);

  const initialPubRes = await ybClient.query(`
    SELECT COUNT(DISTINCT id) as cnt FROM chapters WHERE published_at IS NOT NULL
  `);
  const initialPublishedCount = parseInt(initialPubRes.rows[0].cnt, 10);
  console.log(`Baseline published canonical chapters in Yugabyte: ${initialPublishedCount}`);

  const samples = [];
  const allRss = [];
  const allActiveBuf = [];
  const allReservedBuf = [];
  const backpressureWaits = [];
  const diagnosticDurations = [];
  const diagnosticDownMs = [];
  const diagnosticUpMs = [];
  const diagnosticDbMs = [];
  const diagnosticSemWaitMs = [];
  const diagnosticRlWaitMs = [];

  let oomsObserved = 0;
  let restartsObserved = 0;
  let sampleIndex = 0;

  const seenLogLines = new Set();

  while (Date.now() < END_TIME.getTime()) {
    sampleIndex++;
    const now = new Date();
    const elapsedSec = Math.round((now.getTime() - T0.getTime()) / 1000);

    // 1. Query YugabyteDB
    const pubNowRes = await ybClient.query(`
      SELECT 
        COUNT(DISTINCT id) as unique_published,
        COUNT(*) as total_published
      FROM chapters 
      WHERE published_at >= $1
    `, [T0]);

    const uniquePublished = parseInt(pubNowRes.rows[0].unique_published, 10);
    const totalPublished = parseInt(pubNowRes.rows[0].total_published, 10);

    const qRes = await ybClient.query(`
      SELECT status, count(*) as cnt
      FROM importer_queue
      GROUP BY status
    `);
    const qBreakdown = Object.fromEntries(qRes.rows.map(r => [r.status, parseInt(r.cnt, 10)]));

    const stagedRes = await ybClient.query(`
      SELECT COUNT(*) as staged_cnt
      FROM chapters
      WHERE published_at IS NULL
        AND EXISTS (SELECT 1 FROM pages WHERE pages.chapter_id = chapters.id)
    `);
    const stagedCount = parseInt(stagedRes.rows[0].staged_cnt, 10);

    // 2. Query Discloud logs
    let rawLogs = '';
    try {
      await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded', timeout: 25000 });
      await new Promise(r => setTimeout(r, 2000));
      rawLogs = await page.evaluate(() => {
        const el = document.querySelector('pre, code, .logs, .terminal') || document.body;
        return el ? el.innerText : '';
      });
    } catch (logErr) {
      console.warn(`Warning: failed to scrape logs at sample ${sampleIndex}: ${logErr.message}`);
    }

    let latestRss = null;
    let latestActiveBufMb = null;
    let latestReservedBufMb = null;

    if (rawLogs) {
      const lines = rawLogs.split('\n').map(l => l.trim()).filter(Boolean);
      for (const line of lines) {
        if (!seenLogLines.has(line)) {
          seenLogLines.add(line);

          // Check OOM / crash
          if (line.includes('Falta de RAM') || line.includes('killed process') || line.includes('Out of Memory') || line.includes('SIGKILL')) {
            oomsObserved++;
            console.error(`🚨 OOM DETECTED IN LOG: ${line}`);
          }
          if (line.includes('Initial boot status:') || line.includes('Importer Engine daemon started')) {
            restartsObserved++;
          }

          // Parse [Autotuner STATUS]
          if (line.includes('[Autotuner STATUS]') && line.includes('{')) {
            try {
              const jsonStart = line.indexOf('{');
              const json = JSON.parse(line.slice(jsonStart));
              const meta = json.meta || {};
              if (meta.rssMb !== undefined) {
                latestRss = meta.rssMb;
                allRss.push(meta.rssMb);
              }
              if (meta.bufferedBytes !== undefined) {
                const actMb = meta.bufferedBytes / 1024 / 1024;
                latestActiveBufMb = actMb;
                allActiveBuf.push(actMb);
              }
              if (meta.reservedBytes !== undefined) {
                const resMb = meta.reservedBytes / 1024 / 1024;
                latestReservedBufMb = resMb;
                allReservedBuf.push(resMb);
              }
            } catch {}
          }

          // Parse [Health HEALTHY] Mem: ... rss
          const memMatch = line.match(/Mem:\s*(\d+)MB\s*heap\s*\/\s*(\d+)MB\s*rss/i);
          if (memMatch) {
            const rssVal = parseInt(memMatch[2], 10);
            latestRss = rssVal;
            allRss.push(rssVal);
          }

          // Parse [Memory Backpressure] Admitted after Xms wait
          const bpMatch = line.match(/Admitted after\s*(\d+)ms\s*wait/i);
          if (bpMatch) {
            backpressureWaits.push(parseInt(bpMatch[1], 10));
          }

          // Parse [CHAPTER_DIAGNOSTIC] duration=...ms (down=...ms, up=...ms, db=...ms, sem_wait=...ms, rl_wait=...ms)
          const diagMatch = line.match(/duration=(\d+)ms\s*\(down=(\d+)ms,\s*up=(\d+)ms,\s*db=(\d+)ms,\s*sem_wait=(\d+)ms,\s*rl_wait=(\d+)ms\)/i);
          if (diagMatch) {
            diagnosticDurations.push(parseInt(diagMatch[1], 10));
            diagnosticDownMs.push(parseInt(diagMatch[2], 10));
            diagnosticUpMs.push(parseInt(diagMatch[3], 10));
            diagnosticDbMs.push(parseInt(diagMatch[4], 10));
            diagnosticSemWaitMs.push(parseInt(diagMatch[5], 10));
            diagnosticRlWaitMs.push(parseInt(diagMatch[6], 10));
          }
        }
      }
    }

    const currentRate = elapsedSec > 0 ? parseFloat((uniquePublished / (elapsedSec / 60)).toFixed(2)) : 0;

    const sample = {
      elapsedSec,
      uniquePublished,
      totalPublished,
      currentRate,
      stagedCount,
      qBreakdown,
      latestRss,
      latestActiveBufMb,
      latestReservedBufMb,
    };
    samples.push(sample);

    console.log(
      `[T+${elapsedSec}s | ${Math.round(elapsedSec / 60)}m] ` +
      `Published: ${uniquePublished} unique (${currentRate} cap/min) | ` +
      `Staged: ${stagedCount} | ` +
      `RSS: ${latestRss !== null ? latestRss + 'MB' : 'N/A'} | ` +
      `Buf(Act/Res): ${latestActiveBufMb !== null ? latestActiveBufMb.toFixed(1) + 'MB' : 'N/A'} / ${latestReservedBufMb !== null ? latestReservedBufMb.toFixed(1) + 'MB' : 'N/A'} | ` +
      `Q[IMP: ${qBreakdown['IMPORTING'] || 0}, Q: ${qBreakdown['QUEUED'] || 0}, C: ${qBreakdown['COMPLETED'] || 0}]`
    );

    // Save live checkpoint to file
    fs.writeFileSync(
      'validation_10m_live_observation.json',
      JSON.stringify(
        {
          t0: T0.toISOString(),
          now: now.toISOString(),
          elapsedSec,
          samples,
          allRss,
          allActiveBuf,
          allReservedBuf,
          backpressureWaits,
          diagnosticDurations,
          oomsObserved,
        },
        null,
        2
      )
    );

    // Sleep remaining interval
    const waitMs = Math.max(2000, INTERVAL_SEC * 1000 - 2000);
    if (Date.now() + waitMs < END_TIME.getTime()) {
      await new Promise(r => setTimeout(r, waitMs));
    } else {
      break;
    }
  }

  await browser.close();

  // Final queries
  const finalNow = new Date();
  const actualDurationSec = Math.round((finalNow.getTime() - T0.getTime()) / 1000);
  const actualDurationMin = actualDurationSec / 60;

  const finalPubRes = await ybClient.query(`
    SELECT 
      COUNT(DISTINCT id) as unique_published,
      COUNT(*) as total_published
    FROM chapters 
    WHERE published_at >= $1
  `, [T0]);

  const uniquePublished = parseInt(finalPubRes.rows[0].unique_published, 10);
  const canonicalCapMin = parseFloat((uniquePublished / actualDurationMin).toFixed(2));

  // Memory stats
  const avgRss = allRss.length > 0 ? Math.round(allRss.reduce((a, b) => a + b, 0) / allRss.length) : 0;
  const peakRss = allRss.length > 0 ? Math.max(...allRss) : 0;

  const avgActiveBuf = allActiveBuf.length > 0 ? parseFloat((allActiveBuf.reduce((a, b) => a + b, 0) / allActiveBuf.length).toFixed(2)) : 0;
  const peakActiveBuf = allActiveBuf.length > 0 ? parseFloat(Math.max(...allActiveBuf).toFixed(2)) : 0;

  const avgReservedBuf = allReservedBuf.length > 0 ? parseFloat((allReservedBuf.reduce((a, b) => a + b, 0) / allReservedBuf.length).toFixed(2)) : 0;
  const peakReservedBuf = allReservedBuf.length > 0 ? parseFloat(Math.max(...allReservedBuf).toFixed(2)) : 0;

  // Backpressure wait percentiles
  const sortedWaits = backpressureWaits.slice().sort((a, b) => a - b);
  const bpP50 = sortedWaits.length > 0 ? sortedWaits[Math.floor(sortedWaits.length * 0.5)] : 0;
  const bpP95 = sortedWaits.length > 0 ? sortedWaits[Math.floor(sortedWaits.length * 0.95)] || sortedWaits[sortedWaits.length - 1] : 0;

  // Diagnostics breakdown
  const avgDuration = diagnosticDurations.length > 0 ? Math.round(diagnosticDurations.reduce((a, b) => a + b, 0) / diagnosticDurations.length) : 0;
  const avgDown = diagnosticDownMs.length > 0 ? Math.round(diagnosticDownMs.reduce((a, b) => a + b, 0) / diagnosticDownMs.length) : 0;
  const avgUp = diagnosticUpMs.length > 0 ? Math.round(diagnosticUpMs.reduce((a, b) => a + b, 0) / diagnosticUpMs.length) : 0;
  const avgDb = diagnosticDbMs.length > 0 ? Math.round(diagnosticDbMs.reduce((a, b) => a + b, 0) / diagnosticDbMs.length) : 0;
  const avgSemWait = diagnosticSemWaitMs.length > 0 ? Math.round(diagnosticSemWaitMs.reduce((a, b) => a + b, 0) / diagnosticSemWaitMs.length) : 0;
  const avgRlWait = diagnosticRlWaitMs.length > 0 ? Math.round(diagnosticRlWaitMs.reduce((a, b) => a + b, 0) / diagnosticRlWaitMs.length) : 0;

  // Determine primary bottleneck
  let bottleneck = 'Telegram';
  if (avgRlWait > avgUp && avgRlWait > avgDown) bottleneck = 'source rate limit';
  else if (avgSemWait > avgUp && avgSemWait > avgDown) bottleneck = 'Telegram media semaphore';
  else if (bpP50 > 2000) bottleneck = 'backpressure';
  else if (avgDown > avgUp) bottleneck = 'upstream download';

  const summary = {
    observationWindow: {
      t0: T0.toISOString(),
      tEnd: finalNow.toISOString(),
      durationSeconds: actualDurationSec,
      durationMinutes: actualDurationMin.toFixed(2),
    },
    throughput: {
      uniqueCanonicalPublished: uniquePublished,
      canonicalCapMin,
    },
    memory: {
      avgRssMb: avgRss,
      peakRssMb: peakRss,
      avgActiveBufferedMb: avgActiveBuf,
      peakActiveBufferedMb: peakActiveBuf,
      avgReservedBufferedMb: avgReservedBuf,
      peakReservedBufferedMb: peakReservedBuf,
      backpressureWaitP50Ms: bpP50,
      backpressureWaitP95Ms: bpP95,
      oomsObserved,
      restartsObserved,
    },
    timings: {
      avgChapterDurationMs: avgDuration,
      avgDownloadMs: avgDown,
      avgUploadMs: avgUp,
      avgDbMs: avgDb,
      avgSemWaitMs: avgSemWait,
      avgRateLimitWaitMs: avgRlWait,
    },
    bottleneckDiagnosis: bottleneck,
  };

  fs.writeFileSync('validation_10m_final_summary.json', JSON.stringify(summary, null, 2));

  console.log(`\n============================================================`);
  console.log(`10-MINUTE NORMAL OPERATION OBSERVATION COMPLETE`);
  console.log(`============================================================`);
  console.log(`UNIQUE_CANONICAL_PUBLISHED: ${uniquePublished}`);
  console.log(`CANONICAL_CAP_MIN: ${canonicalCapMin}`);
  console.log(`AVG RSS: ${avgRss} MB`);
  console.log(`PEAK RSS: ${peakRss} MB`);
  console.log(`AVG ACTIVE_BUFFERED: ${avgActiveBuf} MB`);
  console.log(`PEAK ACTIVE_BUFFERED: ${peakActiveBuf} MB`);
  console.log(`AVG RESERVED: ${avgReservedBuf} MB`);
  console.log(`PEAK RESERVED: ${peakReservedBuf} MB`);
  console.log(`MEMORY BACKPRESSURE P50 / P95 WAIT: ${bpP50}ms / ${bpP95}ms`);
  console.log(`OOMS OBSERVED: ${oomsObserved}`);
  console.log(`BOTTLENECK DIAGNOSIS: ${bottleneck}`);
  console.log(`============================================================\n`);

  await ybClient.end();
}

main().catch(err => {
  console.error('Fatal observation error:', err);
  process.exit(1);
});
