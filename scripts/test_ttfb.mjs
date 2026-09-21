import https from 'https';

const agent = new https.Agent({ keepAlive: true, maxSockets: 10 });

const HOME_URL = 'https://manga.project-nox-awerkori.workers.dev/';
const READER_URL = 'https://manga.project-nox-awerkori.workers.dev/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c';
const MEDIA_URL = 'https://manga.project-nox-awerkori.workers.dev/media/000003ed-c2db-4794-bcfa-c5e8b21ce080';

function measureTTFB(url) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = https.get(url, { agent }, (res) => {
      let resolved = false;
      res.once('data', () => {
        if (!resolved) {
          resolved = true;
          const ttfb = Math.round(performance.now() - t0);
          resolve({ status: res.statusCode, ttfb });
        }
      });
      res.on('end', () => {
        if (!resolved) {
          resolved = true;
          resolve({ status: res.statusCode, ttfb: Math.round(performance.now() - t0) });
        }
      });
    });
    req.on('error', (err) => resolve({ status: 500, ttfb: 9999, error: err.message }));
    req.setTimeout(4000, () => { req.destroy(); resolve({ status: 408, ttfb: 9999 }); });
  });
}

async function test() {
  console.log('Testing TTFB with keep-alive...');
  // Warmup connection
  await measureTTFB(HOME_URL);
  await measureTTFB(READER_URL);
  await measureTTFB(MEDIA_URL);

  const home = [];
  const reader = [];
  const media = [];

  for (let i = 0; i < 5; i++) {
    const h = await measureTTFB(HOME_URL);
    const r = await measureTTFB(READER_URL);
    const m = await measureTTFB(MEDIA_URL);
    home.push(h.ttfb);
    reader.push(r.ttfb);
    media.push(m.ttfb);
    await new Promise(res => setTimeout(res, 300));
  }

  console.log('HOME TTFB:', home);
  console.log('READER TTFB:', reader);
  console.log('MEDIA TTFB:', media);
}

test();
