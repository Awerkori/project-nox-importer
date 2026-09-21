import https from 'node:https';

const agent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });

function detailedProbe(url) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    let dnsTime = 0, tcpTime = 0, tlsTime = 0, ttfbTime = 0;
    let reused = false;
    let localPort = 0;

    const req = https.get(url, { agent }, (res) => {
      reused = !!res.socket?.reused;
      localPort = res.socket?.localPort;

      res.once('data', () => {
        ttfbTime = Math.round(performance.now() - t0);
      });
      res.resume();
      res.on('end', () => {
        const total = Math.round(performance.now() - t0);
        resolve({
          status: res.statusCode,
          reused,
          localPort,
          ttfb: ttfbTime || total,
          total
        });
      });
    });

    req.on('socket', (socket) => {
      socket.once('lookup', () => { dnsTime = Math.round(performance.now() - t0); });
      socket.once('connect', () => { tcpTime = Math.round(performance.now() - t0); });
      socket.once('secureConnect', () => { tlsTime = Math.round(performance.now() - t0); });
    });

    req.on('error', (err) => resolve({ status: 500, error: err.message, reused: false }));
    req.setTimeout(5000, () => {
      req.destroy();
      resolve({ status: 408, error: 'TIMEOUT', reused: false });
    });
  });
}

async function main() {
  console.log('Testing 30 sequential READER requests with 200ms gap...');
  const readerUrl = 'https://manga.project-nox-awerkori.workers.dev/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c';
  
  for (let i = 1; i <= 30; i++) {
    const r = await detailedProbe(readerUrl);
    console.log(`[#${i}] status=${r.status}, reused=${r.reused}, port=${r.localPort}, ttfb=${r.ttfb}ms`);
    await new Promise(res => setTimeout(res, 200));
  }
}

main().catch(console.error);
