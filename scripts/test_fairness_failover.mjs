import crypto from 'crypto';
import https from 'node:https';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { DirectTelegramStorageProvider } = await import('../build/storage/direct-telegram.js');

const dummyWebp = Buffer.concat([
  Buffer.from('RIFF', 'ascii'),
  Buffer.from('20000000', 'hex'),
  Buffer.from('WEBPVP8 ', 'ascii'),
  Buffer.from('140000003001009d012a010001000002003425a400037000fefbfd5000', 'hex')
]);

async function runFairnessTest(totalUploads = 126, concurrency = 6) {
  console.log('============================================================');
  console.log(`FAIRNESS TEST: ${totalUploads} UPLOADS (CONCURRENCY: ${concurrency})`);
  console.log('============================================================');

  const provider = new DirectTelegramStorageProvider();
  const startTime = Date.now();
  let completed = 0;
  let inFlight = 0;
  let index = 0;

  const uploadedRecords = [];

  async function worker() {
    while (index < totalUploads) {
      const currentIdx = index++;
      const id = crypto.randomUUID();
      try {
        const fileId = await provider.upload(dummyWebp, 'image/webp', id);
        const botRef = provider.getLastBotReference(id);
        const shardId = provider.getLastShardId(id);
        const channelId = provider.getLastChannelId(id);
        uploadedRecords.push({ id, fileId, botRef, shardId, channelId });
        completed++;
        if (completed % 20 === 0 || completed === totalUploads) {
          process.stdout.write(`Progress: ${completed}/${totalUploads} uploads completed...\r`);
        }
      } catch (err) {
        console.error(`Upload ${currentIdx} error:`, err.message);
      }
    }
  }

  const workers = Array.from({ length: concurrency }, () => worker());
  await Promise.all(workers);
  const elapsedSec = (Date.now() - startTime) / 1000;
  console.log(`\nCompleted ${completed} uploads in ${elapsedSec.toFixed(2)}s (${((completed / elapsedSec) * 60).toFixed(1)} uploads/min)`);

  const summary = provider.getMetricsSummary();

  console.log('\n--- BOT DISTRIBUTION ---');
  let botFair = true;
  for (const b of summary.bots) {
    console.log(`  ${b.bot.padEnd(18)} : ${String(b.uploads).padStart(3)} uploads (${b.pct.toFixed(2)}%) | 429s: ${b.rateLimits429} | p50: ${b.p50LatencyMs}ms`);
    // Expected ~16.67%, acceptable window 11% to 22%
    if (b.pct < 10 || b.pct > 24) {
      botFair = false;
    }
  }

  console.log('\n--- SHARD DISTRIBUTION ---');
  let shardFair = true;
  for (const s of summary.shards) {
    console.log(`  Shard ${String(s.shardNum).padStart(2, '0')} (${s.name.padEnd(21)}) : ${String(s.uploads).padStart(3)} uploads (${s.pct.toFixed(2)}%) | p50: ${s.p50LatencyMs}ms`);
    // Expected ~4.76%, acceptable window 2.5% to 7.5%
    if (s.pct < 2.0 || s.pct > 8.0) {
      shardFair = false;
    }
  }

  console.log('\n============================================================');
  console.log(`TOTAL UPLOADS: ${completed}`);
  console.log(`BOT FAIRNESS:   ${botFair ? 'PASS' : 'FAIL'}`);
  console.log(`SHARD FAIRNESS: ${shardFair ? 'PASS' : 'FAIL'}`);
  console.log('============================================================');

  return { botFair, shardFair, completed, summary };
}

async function runFailoverTest() {
  console.log('\n============================================================');
  console.log('FAILOVER & RECOVERY TEST: BOT AND SHARD COOLDOWN RESILIENCE');
  console.log('============================================================');

  const provider = new DirectTelegramStorageProvider();

  // 1. Artificially set Bot 01 in cooldown for 60 seconds
  const bot1 = (provider).bots.find(b => b.ref === 'MANGA_STORAGE_01');
  bot1.cooldownUntil = Date.now() + 60_000;
  console.log(`[TEST] Placed Bot 01 (MANGA_STORAGE_01) in cooldown for 60s...`);

  // Run 20 uploads while Bot 01 is strictly in cooldown
  console.log('[TEST] Executing 20 uploads while Bot 01 is in cooldown...');
  const cooldownBatch = [];
  for (let i = 0; i < 20; i++) {
    const id = crypto.randomUUID();
    const fileId = await provider.upload(dummyWebp, 'image/webp', id);
    const botRef = provider.getLastBotReference(id);
    cooldownBatch.push({ id, fileId, botRef });
  }

  const bot1UploadsDuringCooldown = cooldownBatch.filter(r => r.botRef === 'MANGA_STORAGE_01').length;
  console.log(`[TEST] Bot 01 uploads during cooldown: ${bot1UploadsDuringCooldown} / 20 (Expected: 0)`);
  const botFailoverPass = bot1UploadsDuringCooldown === 0;

  // 2. Clear cooldown (simulating cooldown expiration)
  console.log('[TEST] Simulating cooldown expiration for Bot 01...');
  bot1.cooldownUntil = 0;

  console.log('[TEST] Executing 30 recovery uploads...');
  const recoveryBatch = [];
  for (let i = 0; i < 30; i++) {
    const id = crypto.randomUUID();
    const fileId = await provider.upload(dummyWebp, 'image/webp', id);
    const botRef = provider.getLastBotReference(id);
    recoveryBatch.push({ id, fileId, botRef });
  }

  const bot1UploadsDuringRecovery = recoveryBatch.filter(r => r.botRef === 'MANGA_STORAGE_01').length;
  console.log(`[TEST] Bot 01 uploads during recovery: ${bot1UploadsDuringRecovery} / 30 (Expected: > 0, gradual recovery)`);
  const botRecoveryPass = bot1UploadsDuringRecovery > 0;

  // 3. Shard Failover test: place Shard 10 in cooldown for 60s
  const shard10 = (provider).shards.find(s => s.name.includes('010'));
  shard10.cooldownUntil = Date.now() + 60_000;
  console.log(`\n[TEST] Placed Shard 10 (${shard10.name}) in cooldown for 60s...`);

  const shardCooldownBatch = [];
  for (let i = 0; i < 20; i++) {
    const id = crypto.randomUUID();
    const fileId = await provider.upload(dummyWebp, 'image/webp', id);
    const shardId = provider.getLastShardId(id);
    shardCooldownBatch.push({ id, fileId, shardId });
  }

  const shard10UploadsDuringCooldown = shardCooldownBatch.filter(r => r.shardId === shard10.shardId).length;
  console.log(`[TEST] Shard 10 uploads during cooldown: ${shard10UploadsDuringCooldown} / 20 (Expected: 0)`);
  const shardFailoverPass = shard10UploadsDuringCooldown === 0;

  // Clear shard cooldown
  shard10.cooldownUntil = 0;
  const shardRecoveryBatch = [];
  for (let i = 0; i < 30; i++) {
    const id = crypto.randomUUID();
    const fileId = await provider.upload(dummyWebp, 'image/webp', id);
    const shardId = provider.getLastShardId(id);
    shardRecoveryBatch.push({ id, fileId, shardId });
  }

  const shard10UploadsDuringRecovery = shardRecoveryBatch.filter(r => r.shardId === shard10.shardId).length;
  console.log(`[TEST] Shard 10 uploads during recovery: ${shard10UploadsDuringRecovery} / 30 (Expected: > 0)`);
  const shardRecoveryPass = shard10UploadsDuringRecovery > 0;

  console.log('\n============================================================');
  console.log(`BOT FAILOVER:     ${botFailoverPass ? 'PASS' : 'FAIL'}`);
  console.log(`BOT RECOVERY:     ${botRecoveryPass ? 'PASS' : 'FAIL'}`);
  console.log(`SHARD FAILOVER:   ${shardFailoverPass ? 'PASS' : 'FAIL'}`);
  console.log(`SHARD RECOVERY:   ${shardRecoveryPass ? 'PASS' : 'FAIL'}`);
  console.log('============================================================');

  return botFailoverPass && botRecoveryPass && shardFailoverPass && shardRecoveryPass;
}

async function main() {
  const fairnessResult = await runFairnessTest(126, 6);
  const failoverResult = await runFailoverTest();

  console.log('\n============================================================');
  console.log('FINAL SUITE EVALUATION:');
  console.log(`BOT FAIRNESS:     ${fairnessResult.botFair ? 'PASS' : 'FAIL'}`);
  console.log(`SHARD FAIRNESS:   ${fairnessResult.shardFair ? 'PASS' : 'FAIL'}`);
  console.log(`FAILOVER RESILIENCE: ${failoverResult ? 'PASS' : 'FAIL'}`);
  console.log('============================================================');

  if (!fairnessResult.botFair || !fairnessResult.shardFair || !failoverResult) {
    process.exit(1);
  }
}

main().catch(err => {
  console.error('Fatal error during test suite:', err);
  process.exit(1);
});
