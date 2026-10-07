import https from 'node:https';

const BASE_URL = 'https://manga.project-nox-awerkori.workers.dev';
const agent = new https.Agent({ keepAlive: true, maxSockets: 5 });

async function probe(path) {
  const url = `${BASE_URL}${path}`;
  const start = performance.now();
  return new Promise((resolve) => {
    const req = https.get(url, { agent, headers: { 'User-Agent': 'NoxAuditor/1.0' } }, (res) => {
      const ttfb = Math.round(performance.now() - start);
      const cfCache = res.headers['cf-cache-status'] || 'NONE';
      const xCache = res.headers['x-nox-cache'] || 'NONE';
      let dataLen = 0;
      res.on('data', chunk => dataLen += chunk.length);
      res.on('end', () => {
        const total = Math.round(performance.now() - start);
        resolve({ path, statusCode: res.statusCode, ttfb, total, cfCache, xCache, bytes: dataLen });
      });
    });
    req.on('error', (err) => resolve({ path, error: err.message }));
  });
}

async function main() {
  console.log("Measuring Obra route latency after optimization deployment...");
  // 5 probes in sequence
  for (let i = 1; i <= 5; i++) {
    const res = await probe('/obra/imperador-magico');
    console.log(`Probe #${i}:`, res);
    await new Promise(r => setTimeout(r, 500));
  }

  console.log("\nMeasuring Home route latency...");
  for (let i = 1; i <= 3; i++) {
    const res = await probe('/');
    console.log(`Home #${i}:`, res);
    await new Promise(r => setTimeout(r, 500));
  }

  console.log("\nMeasuring Reader route latency...");
  for (let i = 1; i <= 3; i++) {
    const res = await probe('/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c');
    console.log(`Reader #${i}:`, res);
    await new Promise(r => setTimeout(r, 500));
  }
}

main().catch(console.error);
