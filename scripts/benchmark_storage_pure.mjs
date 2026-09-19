import crypto from 'crypto';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { DirectTelegramStorageProvider } = await import('../build/storage/direct-telegram.js');

// Typical manga page size (around 120 KB realistic WebP payload)
const samplePageWebp = Buffer.alloc(120 * 1024);
// Add valid WebP header
Buffer.from('RIFF', 'ascii').copy(samplePageWebp, 0);
samplePageWebp.writeUInt32LE(120 * 1024 - 8, 4);
Buffer.from('WEBPVP8 ', 'ascii').copy(samplePageWebp, 8);
samplePageWebp.writeUInt32LE(120 * 1024 - 20, 16);
Buffer.from('3001009d012a010001000002003425a400037000fefbfd5000', 'hex').copy(samplePageWebp, 20);

async function runSustainedBenchmark(durationSeconds = 45, concurrency = 12) {
  console.log('============================================================');
  console.log(`PURE STORAGE BENCHMARK: ${durationSeconds}s SUSTAINED @ CONCURRENCY ${concurrency}`);
  console.log(`Page Payload Size: ${(samplePageWebp.length / 1024).toFixed(1)} KB`);
  console.log('============================================================');

  const provider = new DirectTelegramStorageProvider();
  const startTime = Date.now();
  const stopTime = startTime + (durationSeconds * 1000);

  let totalPages = 0;
  let totalBytes = 0;
  let tlsErrors = 0;
  let rateLimits429 = 0;
  let running = true;

  async function worker(workerId) {
    while (Date.now() < stopTime && running) {
      const id = crypto.randomUUID();
      try {
        await provider.upload(samplePageWebp, 'image/webp', id);
        totalPages++;
        totalBytes += samplePageWebp.length;
      } catch (err) {
        if (err.statusCode === 429) {
          rateLimits429++;
        } else if (err.message && (err.message.includes('TLS') || err.message.includes('ECONNRESET') || err.message.includes('ETIMEDOUT'))) {
          tlsErrors++;
        }
      }
    }
  }

  const workers = Array.from({ length: concurrency }, (_, i) => worker(i + 1));

  // Progress reporter
  const interval = setInterval(() => {
    const elapsedSec = (Date.now() - startTime) / 1000;
    if (elapsedSec > 0) {
      const currentRate = (totalPages / elapsedSec) * 60;
      const currentMBmin = ((totalBytes / (1024 * 1024)) / elapsedSec) * 60;
      process.stdout.write(`Elapsed: ${elapsedSec.toFixed(0)}s | Pages: ${totalPages} | Rate: ${currentRate.toFixed(1)} pages/min | Bandwidth: ${currentMBmin.toFixed(2)} MB/min\r`);
    }
  }, 1000);

  await Promise.all(workers);
  clearInterval(interval);

  const totalElapsedSec = (Date.now() - startTime) / 1000;
  const pagesPerMin = (totalPages / totalElapsedSec) * 60;
  const mbPerMin = ((totalBytes / (1024 * 1024)) / totalElapsedSec) * 60;

  const summary = provider.getMetricsSummary();

  console.log('\n\n============================================================');
  console.log('BENCHMARK EXECUTION RESULTS');
  console.log('============================================================');
  console.log(`TOTAL PAGES UPLOADED : ${totalPages}`);
  console.log(`TOTAL ELAPSED TIME   : ${totalElapsedSec.toFixed(2)}s`);
  console.log(`SUSTAINED RATE       : ${pagesPerMin.toFixed(1)} pages/min (Target: 400+)`);
  console.log(`BANDWIDTH            : ${mbPerMin.toFixed(2)} MB/min`);
  console.log(`HTTP 429 FLOODWAIT   : ${rateLimits429}`);
  console.log(`TLS / NETWORK ERRORS : ${tlsErrors}`);
  console.log('------------------------------------------------------------');

  console.log('BOT DISTRIBUTION:');
  for (const b of summary.bots) {
    console.log(`  ${b.bot.padEnd(18)} : ${String(b.uploads).padStart(4)} pages (${b.pct.toFixed(1)}%) | p50: ${b.p50LatencyMs}ms | p95: ${b.p95LatencyMs}ms | 429s: ${b.rateLimits429}`);
  }

  console.log('\nSHARD DISTRIBUTION (21 Shards):');
  for (const s of summary.shards) {
    console.log(`  Shard ${String(s.shardNum).padStart(2, '0')} (${s.name.padEnd(21)}) : ${String(s.uploads).padStart(3)} pages (${s.pct.toFixed(1)}%) | p50: ${s.p50LatencyMs}ms`);
  }

  console.log('============================================================');
  const targetMet = pagesPerMin >= 300;
  console.log(`STORAGE BENCHMARK STATUS: ${targetMet ? 'PASS' : 'FAIL'} (${pagesPerMin >= 400 ? 'EXCEEDED 400 TARGET' : 'PASSED 300 MINIMUM'})`);
  console.log('============================================================');

  return {
    totalPages,
    totalElapsedSec,
    pagesPerMin,
    mbPerMin,
    rateLimits429,
    tlsErrors,
    targetMet,
    summary
  };
}

async function main() {
  // Test Level: 45s sustained with 12 concurrency
  const result = await runSustainedBenchmark(45, 12);
  if (!result.targetMet) {
    process.exit(1);
  }
}

main().catch(err => {
  console.error('Benchmark fatal error:', err);
  process.exit(1);
});
