import https from 'node:https';
import fs from 'node:fs';

const keepAliveAgent = new https.Agent({ keepAlive: true, maxSockets: 10 });
const freshAgent = false;

const HOME_URL = 'https://manga.project-nox-awerkori.workers.dev/';
const SAMPLE_READER_CHAPTERS = [
  '46b7538b-fcb8-40ec-b3ee-cdadd2edb04c',
  '1fc72605-e4b7-4db4-ab9f-7ec7e466b02a',
  '902047ff-7c43-4dc9-aa32-ca0db1dca3ba'
];
const SAMPLE_MEDIA_IDS = [
  '000003ed-c2db-4794-bcfa-c5e8b21ce080',
  '00000624-a749-43c2-b5e1-7cae0b9fb419',
  '00000650-25ef-4ff6-8cbb-fcb436940e4f'
];

function probe(url, agent, timeoutMs = 4000) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    let ttfb = 0;
    const req = https.get(url, {
      agent,
      headers: {
        'Connection': agent ? 'keep-alive' : 'close',
        'User-Agent': 'NoxDualMonitor/2.0'
      }
    }, (res) => {
      res.once('data', () => {
        ttfb = Math.round(performance.now() - t0);
      });
      res.resume();
      res.on('end', () => {
        if (!ttfb) ttfb = Math.round(performance.now() - t0);
        resolve({ ttfb, status: res.statusCode, cfCache: res.headers['cf-cache-status'] || 'NONE' });
      });
    });
    req.on('error', (err) => resolve({ ttfb: 9999, status: 500, error: err.message }));
    req.setTimeout(timeoutMs, () => {
      req.destroy();
      resolve({ ttfb: 9999, status: 408, error: 'TIMEOUT' });
    });
  });
}

function percentile(arr, p) {
  if (!arr || !arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
}

async function main() {
  console.log('[Dual-Probe Monitor] Started. Measuring Fresh vs Keepalive continuously.');
  const readerFresh = [];
  const readerKeepalive = [];
  const mediaFresh = [];
  const mediaKeepalive = [];
  const homeFresh = [];
  const homeKeepalive = [];

  const outPath = '/home/awerkori/.Projects/project-nox-importer/validation_dual_latency_progress.json';

  let idx = 0;
  while (true) {
    idx++;
    const readerUrl = `https://manga.project-nox-awerkori.workers.dev/ler/${SAMPLE_READER_CHAPTERS[idx % SAMPLE_READER_CHAPTERS.length]}`;
    const mediaUrl = `https://manga.project-nox-awerkori.workers.dev/media/${SAMPLE_MEDIA_IDS[idx % SAMPLE_MEDIA_IDS.length]}`;

    // 1. Keepalive probes
    const hKa = await probe(HOME_URL, keepAliveAgent);
    const rKa = await probe(readerUrl, keepAliveAgent);
    const mKa = await probe(mediaUrl, keepAliveAgent);

    if (hKa.ttfb < 9000) homeKeepalive.push(hKa.ttfb);
    if (rKa.ttfb < 9000) readerKeepalive.push(rKa.ttfb);
    if (mKa.ttfb < 9000) mediaKeepalive.push(mKa.ttfb);

    // 2. Fresh probes
    const hFr = await probe(HOME_URL, freshAgent);
    const rFr = await probe(readerUrl, freshAgent);
    const mFr = await probe(mediaUrl, freshAgent);

    if (hFr.ttfb < 9000) homeFresh.push(hFr.ttfb);
    if (rFr.ttfb < 9000) readerFresh.push(rFr.ttfb);
    if (mFr.ttfb < 9000) mediaFresh.push(mFr.ttfb);

    const stats = {
      sampleCount: idx,
      lastUpdated: new Date().toISOString(),
      reader: {
        keepaliveP50: percentile(readerKeepalive, 0.5),
        keepaliveP95: percentile(readerKeepalive, 0.95),
        freshP50: percentile(readerFresh, 0.5),
        freshP95: percentile(readerFresh, 0.95),
        samples: readerKeepalive.length
      },
      media: {
        keepaliveP50: percentile(mediaKeepalive, 0.5),
        keepaliveP95: percentile(mediaKeepalive, 0.95),
        freshP50: percentile(mediaFresh, 0.5),
        freshP95: percentile(mediaFresh, 0.95),
        samples: mediaKeepalive.length
      },
      home: {
        keepaliveP50: percentile(homeKeepalive, 0.5),
        keepaliveP95: percentile(homeKeepalive, 0.95),
        freshP50: percentile(homeFresh, 0.5),
        freshP95: percentile(homeFresh, 0.95),
        samples: homeKeepalive.length
      }
    };

    fs.writeFileSync(outPath, JSON.stringify(stats, null, 2));

    await new Promise((r) => setTimeout(r, 5000));
  }
}

main().catch(console.error);
