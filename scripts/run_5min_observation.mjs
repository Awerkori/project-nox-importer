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
  const DURATION_SEC = 300; // 5 minutes
  const INTERVAL_SEC = 25;  // Sample every 25s
  const END_TIME = new Date(T0.getTime() + DURATION_SEC * 1000);

  console.log(`\n============================================================`);
  console.log(`STARTING 5-MINUTE NORMAL OPERATION OBSERVATION ON DISCLOUD`);
  console.log(`T0: ${T0.toISOString()}`);
  console.log(`Planned End: ${END_TIME.toISOString()} (${DURATION_SEC}s)`);
  console.log(`Monitoring PR #73 Streaming Buffer Reservation & Commit Invariant`);
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
  const allCommittedBuf = [];
  const allMaxCommittedObserved = [];
  const backpressureWaits = [];
  let oomDetected = 0;

  let sampleIndex = 0;

  while (new Date() < END_TIME) {
    sampleIndex++;
    const sampleTime = new Date();
    const elapsedSec = Math.round((sampleTime.getTime() - T0.getTime()) / 1000);

    let logsText = '';
    try {
      await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded', timeout: 20000 });
      await new Promise(r => setTimeout(r, 2000));
      logsText = await page.evaluate(() => {
        const el = document.querySelector('pre, code, .logs, .terminal') || document.body;
        return el.innerText;
      });
    } catch (pageErr) {
      console.warn(`[Sample ${sampleIndex}] Log fetch error: ${pageErr.message}`);
    }

    const lines = logsText.split('\n').map(l => l.trim()).filter(Boolean);

    // Parse Pipeline capacity and autotuner logs
    let latestRss = null;
    let latestActiveBuf = null;
    let latestReservedBuf = null;
    let latestCommittedBuf = null;
    let latestMaxObserved = null;
    let latestConcurrency = null;

    for (let i = lines.length - 1; i >= 0; i--) {
      const line = lines[i];

      if (line.includes('Pipeline capacity') && latestActiveBuf === null) {
        try {
          const jsonStart = line.indexOf('{');
          if (jsonStart >= 0) {
            const parsed = JSON.parse(line.slice(jsonStart));
            latestActiveBuf = (parsed.bufferedBytes || 0) / (1024 * 1024);
            latestReservedBuf = (parsed.reservedBytes || 0) / (1024 * 1024);
            latestCommittedBuf = (parsed.committedBytes || 0) / (1024 * 1024);
            latestMaxObserved = (parsed.bufferBudgetMaxObserved || 0) / (1024 * 1024);
            latestRss = parsed.rssMb || null;
            latestConcurrency = parsed.chapterConcurrency || null;
          }
        } catch {}
      }

      if (line.includes('[Autotuner Telemetry]') && latestRss === null) {
        const rssMatch = line.match(/(\d+)MB rss/);
        if (rssMatch) latestRss = parseInt(rssMatch[1], 10);
      }

      if (line.includes('Memory Backpressure') && line.includes('Admitted after')) {
        const waitMatch = line.match(/Admitted after (\d+)ms wait/);
        if (waitMatch) {
          backpressureWaits.push(parseInt(waitMatch[1], 10));
        }
      }

      if (line.toLowerCase().includes('oom') || line.toLowerCase().includes('out of memory') || line.toLowerCase().includes('falta de ram')) {
        oomDetected++;
      }
    }

    // Check DB publications count
    const currentPubRes = await ybClient.query(`
      SELECT COUNT(DISTINCT id) as cnt FROM chapters WHERE published_at IS NOT NULL
    `);
    const currentPublishedCount = parseInt(currentPubRes.rows[0].cnt, 10);
    const newPublished = currentPublishedCount - initialPublishedCount;
    const currentCapMin = elapsedSec > 0 ? ((newPublished / elapsedSec) * 60).toFixed(2) : '0.00';

    if (latestRss !== null) allRss.push(latestRss);
    if (latestActiveBuf !== null) allActiveBuf.push(latestActiveBuf);
    if (latestReservedBuf !== null) allReservedBuf.push(latestReservedBuf);
    if (latestCommittedBuf !== null) allCommittedBuf.push(latestCommittedBuf);
    if (latestMaxObserved !== null) allMaxCommittedObserved.push(latestMaxObserved);

    console.log(
      `[Sample ${sampleIndex} | ${elapsedSec}s/${DURATION_SEC}s] ` +
      `New Pubs: ${newPublished} (${currentCapMin} cap/min) | ` +
      `RSS: ${latestRss !== null ? latestRss + 'MB' : 'N/A'} | ` +
      `ActiveBuf: ${latestActiveBuf !== null ? latestActiveBuf.toFixed(1) + 'MB' : 'N/A'} | ` +
      `ReservedBuf: ${latestReservedBuf !== null ? latestReservedBuf.toFixed(1) + 'MB' : 'N/A'} | ` +
      `Committed: ${latestCommittedBuf !== null ? latestCommittedBuf.toFixed(1) + 'MB' : 'N/A'} | ` +
      `MaxObserved: ${latestMaxObserved !== null ? latestMaxObserved.toFixed(1) + 'MB' : 'N/A'} | ` +
      `OOMs: ${oomDetected}`
    );

    samples.push({
      elapsedSec,
      newPublished,
      currentCapMin: parseFloat(currentCapMin),
      rss: latestRss,
      activeBufMb: latestActiveBuf,
      reservedBufMb: latestReservedBuf,
      committedBufMb: latestCommittedBuf,
      maxObservedMb: latestMaxObserved,
      concurrency: latestConcurrency,
    });

    const remainingSec = Math.round((END_TIME.getTime() - new Date().getTime()) / 1000);
    if (remainingSec > 0) {
      await new Promise(r => setTimeout(r, Math.min(INTERVAL_SEC, remainingSec) * 1000));
    }
  }

  await browser.close();

  // Final DB snapshot
  const finalPubRes = await ybClient.query(`
    SELECT COUNT(DISTINCT id) as cnt FROM chapters WHERE published_at IS NOT NULL
  `);
  const finalPublishedCount = parseInt(finalPubRes.rows[0].cnt, 10);
  const totalNewPublished = finalPublishedCount - initialPublishedCount;
  const actualDurationSec = Math.round((new Date().getTime() - T0.getTime()) / 1000);
  const canonicalCapMin = ((totalNewPublished / actualDurationSec) * 60).toFixed(2);

  await ybClient.end();

  const avgRss = allRss.length > 0 ? (allRss.reduce((a, b) => a + b, 0) / allRss.length).toFixed(1) : 'N/A';
  const peakRss = allRss.length > 0 ? Math.max(...allRss) : 'N/A';
  const avgActive = allActiveBuf.length > 0 ? (allActiveBuf.reduce((a, b) => a + b, 0) / allActiveBuf.length).toFixed(1) : 'N/A';
  const peakActive = allActiveBuf.length > 0 ? Math.max(...allActiveBuf).toFixed(1) : 'N/A';
  const avgReserved = allReservedBuf.length > 0 ? (allReservedBuf.reduce((a, b) => a + b, 0) / allReservedBuf.length).toFixed(1) : 'N/A';
  const peakReserved = allReservedBuf.length > 0 ? Math.max(...allReservedBuf).toFixed(1) : 'N/A';
  const peakCommitted = allCommittedBuf.length > 0 ? Math.max(...allCommittedBuf).toFixed(1) : 'N/A';
  const peakMaxObserved = allMaxCommittedObserved.length > 0 ? Math.max(...allMaxCommittedObserved).toFixed(1) : 'N/A';

  const summary = {
    durationSec: actualDurationSec,
    totalNewPublished,
    canonicalCapMin: parseFloat(canonicalCapMin),
    avgRssMb: avgRss,
    peakRssMb: peakRss,
    avgActiveMb: avgActive,
    peakActiveMb: peakActive,
    avgReservedMb: avgReserved,
    peakReservedMb: peakReserved,
    peakCommittedMb: peakCommitted,
    peakMaxObservedMb: peakMaxObserved,
    oomCount: oomDetected,
    samplesCount: samples.length,
    samples,
  };

  fs.writeFileSync('validation_5m_streaming_summary.json', JSON.stringify(summary, null, 2));

  console.log(`\n============================================================`);
  console.log(`5-MINUTE OBSERVATION COMPLETED`);
  console.log(`============================================================`);
  console.log(`TOTAL DURATION: ${actualDurationSec}s`);
  console.log(`UNIQUE CANONICAL PUBLISHED: ${totalNewPublished}`);
  console.log(`CANONICAL CAP/MIN: ${canonicalCapMin}`);
  console.log(`AVG RSS: ${avgRss} MB | PEAK RSS: ${peakRss} MB`);
  console.log(`AVG ACTIVE BUFFER: ${avgActive} MB | PEAK ACTIVE: ${peakActive} MB`);
  console.log(`AVG RESERVED: ${avgReserved} MB | PEAK RESERVED: ${peakReserved} MB`);
  console.log(`PEAK COMMITTED (ACTIVE + RESERVED): ${peakCommitted} MB`);
  console.log(`MAX COMMITTED OBSERVED: ${peakMaxObserved} MB`);
  console.log(`OOMs DETECTED: ${oomDetected}`);
  console.log(`MAX_BUFFERED_BYTES (64 MB) RESPECTED: ${peakCommitted !== 'N/A' && parseFloat(peakCommitted) <= 64 ? 'YES' : 'NO'}`);
  console.log(`============================================================\n`);
}

main().catch(console.error);
