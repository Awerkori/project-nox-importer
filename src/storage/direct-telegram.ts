import https from 'node:https';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { StorageProvider } from './provider.js';
import { Logger } from '../core/logger.js';

export interface BotRuntimeMetrics {
  ref: string;
  token: string;
  username: string;
  activeUploads: number;
  totalUploads: number;
  successes: number;
  failures: number;
  rateLimits429: number;
  floodWaitSeconds: number;
  cooldownUntil: number;
  maxConcurrent: number;
  consecutiveSuccesses: number;
  recentUploadTimestamps: number[];
  latencies: number[];
}

export interface ShardRuntimeMetrics {
  shardId: string;
  channelId: string;
  name: string;
  activeUploads: number;
  totalUploads: number;
  successes: number;
  failures: number;
  rateLimits429: number;
  cooldownUntil: number;
  maxConcurrent: number;
  recentUploadTimestamps: number[];
  latencies: number[];
}

export interface UploadRecordMetadata {
  botRef: string;
  shardId: string;
  channelId: string;
  fileId: string;
  timestamp: number;
  byteSize: number;
  latencyMs: number;
}

export class BandwidthLimiter {
  private bytesPerSec: number;
  private maxBurst: number;
  private tokens: number;
  private lastRefill: number = Date.now();
  private waitChain: Promise<void> = Promise.resolve();

  constructor(bytesPerSec: number = 4.0 * 1024 * 1024, maxBurst?: number) {
    this.bytesPerSec = bytesPerSec;
    this.maxBurst = maxBurst || Math.max(bytesPerSec, 2 * 1024 * 1024);
    this.tokens = this.maxBurst;
  }

  async acquire(bytes: number): Promise<void> {
    const acquireInternal = async () => {
      const now = Date.now();
      const elapsed = Math.max(0, (now - this.lastRefill) / 1000);
      this.tokens = Math.min(this.maxBurst, this.tokens + elapsed * this.bytesPerSec);
      this.lastRefill = now;

      if (this.tokens >= bytes) {
        this.tokens -= bytes;
        return;
      }

      const deficit = bytes - this.tokens;
      const waitMs = Math.ceil((deficit / this.bytesPerSec) * 1000);
      this.tokens = 0;
      this.lastRefill = now + waitMs;
      await new Promise((r) => setTimeout(r, waitMs));
    };

    this.waitChain = this.waitChain.then(acquireInternal, acquireInternal);
    return this.waitChain;
  }

  setRate(bytesPerSec: number) {
    this.bytesPerSec = bytesPerSec;
    this.maxBurst = Math.max(bytesPerSec, 2 * 1024 * 1024);
  }
}

export class DirectTelegramStorageProvider implements StorageProvider {
  private logger = new Logger('DirectTelegramStorage');
  private httpsAgent: https.Agent;
  private bandwidthLimiter: BandwidthLimiter;
  private bots: BotRuntimeMetrics[] = [];
  private shards: ShardRuntimeMetrics[] = [];
  private metadataMap = new Map<string, UploadRecordMetadata>();
  private lastBotRef: string = 'MANGA_STORAGE_01';
  private lastShardId: string | null = null;
  private lastChannelId: string | null = null;
  private botRoundRobinIndex = 0;
  private shardRoundRobinIndex = 0;

  // Bounded queue backpressure
  private maxGlobalConcurrent = 32;
  private currentGlobalActive = 0;
  private waitingQueue: Array<() => void> = [];

  constructor(checkpointPath?: string, rateLimitBytesPerSec?: number) {
    const rate = rateLimitBytesPerSec || (process.env.UPLOAD_RATE_LIMIT_BYTES_PER_SEC ? parseInt(process.env.UPLOAD_RATE_LIMIT_BYTES_PER_SEC, 10) : 4.0 * 1024 * 1024);
    this.bandwidthLimiter = new BandwidthLimiter(rate, Math.max(rate, 2 * 1024 * 1024));
    this.logger.info(`DirectTelegramStorage BandwidthLimiter configured: ${(rate / (1024 * 1024)).toFixed(1)} MB/s`);

    this.httpsAgent = new https.Agent({
      keepAlive: true,
      maxSockets: 64,
      maxFreeSockets: 16,
      timeout: 60_000,
      keepAliveMsecs: 30_000,
    });

    this.loadConfiguration(checkpointPath);
  }

  private loadConfiguration(checkpointPath?: string): void {
    let cp: any = null;

    if (process.env.STORAGE_CHECKPOINT_JSON) {
      try {
        const raw = process.env.STORAGE_CHECKPOINT_JSON.trim();
        cp = JSON.parse(raw.startsWith('{') ? raw : Buffer.from(raw, 'base64').toString('utf8'));
      } catch (e) {
        this.logger.warn('Failed to parse STORAGE_CHECKPOINT_JSON env var');
      }
    }

    if (!cp) {
      const candidates = [
        checkpointPath,
        path.join(process.cwd(), 'config', 'storage_checkpoint.json'),
        path.join(os.homedir(), '.config', 'project-nox', 'storage_checkpoint.json'),
        '/app/config/storage_checkpoint.json',
      ].filter(Boolean) as string[];

      for (const p of candidates) {
        if (fs.existsSync(p)) {
          try {
            cp = JSON.parse(fs.readFileSync(p, 'utf8'));
            this.logger.info(`Loaded storage checkpoint from ${p}`);
            break;
          } catch (e) { /* ignore */ }
        }
      }
    }

    const defaultBots: Record<string, { token: string; username: string }> = {
      'Project Nox Storage 01': { token: '8618893801:AAEih6jadpEkXdsjKY1JVDAzvXwWl9RMy1g', username: 'project_nox_manga_storage_bot' },
      'Project Nox Storage 02': { token: '8511822349:AAGSjzMAx9kptEckfYLIiMwSiKa4IgnL_vg', username: 'nox_manga_02_bot' },
      'Project Nox Storage 03': { token: '8943758747:AAHltzHOycs539RgljKdqB9wcJjlEihhthA', username: 'project_nox_storage_03_bot' },
      'Project Nox Storage 04': { token: '8929999439:AAFQppMRv4TRf6Cpwb9Dk_9B2wvWYuJxDbw', username: 'project_nox_storage_04_bot' },
      'Project Nox Storage 05': { token: '8650821577:AAEXH-Uw3XW2FbIypGY03la_rVWVMvbqPWI', username: 'project_nox_storage_05_bot' },
      'Project Nox Storage 06': { token: '8782900571:AAFi0eRd7wsK8yo44QP0cacXiw4pNfpc-h0', username: 'project_nox_storage_06_bot' },
    };

    const defaultShards10To21: Record<string, { shardId: string; channel_id: number }> = {
      SHARD_10: { shardId: 'fd879adf-fe05-497d-b29b-e6c7624abbc3', channel_id: -1003979364862 },
      SHARD_11: { shardId: '786a6616-dcff-4b3e-9eca-2464c483dc93', channel_id: -1003948859445 },
      SHARD_12: { shardId: 'e286cd32-14d8-4626-bfa2-3a915353eb69', channel_id: -1003631700705 },
      SHARD_13: { shardId: '1f12c6db-c9a4-4baf-b70b-7715d1e08e5c', channel_id: -1003596443728 },
      SHARD_14: { shardId: '949ff64e-1cbe-45a9-9a0b-9326468d9715', channel_id: -1004365804735 },
      SHARD_15: { shardId: '067a30cc-385e-404e-9e40-fa4edb3c59e4', channel_id: -1003485897860 },
      SHARD_16: { shardId: 'c58339e8-c3d3-4648-a807-d3d504d706f8', channel_id: -1004371483636 },
      SHARD_17: { shardId: '5a846253-d5ee-455f-aa1a-f391f518c29f', channel_id: -1003716637832 },
      SHARD_18: { shardId: '17939436-908f-42da-83d5-e9e84728f56e', channel_id: -1003942674189 },
      SHARD_19: { shardId: 'c13a4c37-35bf-4938-9e0d-c8e9925bfee9', channel_id: -1004438265407 },
      SHARD_20: { shardId: '4fa27898-ff50-4fa0-9e63-e44c53b89300', channel_id: -1003947396569 },
      SHARD_21: { shardId: 'fde71d17-a969-4527-9d09-933fa62d465f', channel_id: -1004351618982 },
    };

    const botDefs = [
      { key: 'Project Nox Storage 01', ref: 'MANGA_STORAGE_01', defaultUser: 'project_nox_manga_storage_bot' },
      { key: 'Project Nox Storage 02', ref: 'MANGA_STORAGE_2',  defaultUser: 'nox_manga_02_bot' },
      { key: 'Project Nox Storage 03', ref: 'MANGA_STORAGE_03', defaultUser: 'project_nox_storage_03_bot' },
      { key: 'Project Nox Storage 04', ref: 'MANGA_STORAGE_04', defaultUser: 'project_nox_storage_04_bot' },
      { key: 'Project Nox Storage 05', ref: 'MANGA_STORAGE_05', defaultUser: 'project_nox_storage_05_bot' },
      { key: 'Project Nox Storage 06', ref: 'MANGA_STORAGE_06', defaultUser: 'project_nox_storage_06_bot' },
    ];

    for (const def of botDefs) {
      const bdata = cp?.bots?.[def.key];
      const token = bdata?.token || process.env[`TELEGRAM_STORAGE_BOT_${def.ref.replace('MANGA_STORAGE_', '')}_TOKEN`] || defaultBots[def.key]?.token;
      if (!token) {
        throw new Error(`Missing token for bot ${def.ref} (${def.key})`);
      }
      this.bots.push({
        ref: def.ref,
        token,
        username: bdata?.username || def.defaultUser,
        activeUploads: 0,
        totalUploads: 0,
        successes: 0,
        failures: 0,
        rateLimits429: 0,
        floodWaitSeconds: 0,
        cooldownUntil: 0,
        maxConcurrent: 4,
        consecutiveSuccesses: 0,
        recentUploadTimestamps: [],
        latencies: [],
      });
    }

    const shardNames: Record<string, string> = {
      SHARD_01: 'Nox Manga Storage 001',
      SHARD_02: 'Nox Manga Storage 002',
      SHARD_03: 'Nox Manga Storage 003',
      SHARD_04: 'Nox Manga Storage 004',
      SHARD_05: 'Nox Manga Storage 005',
      SHARD_06: 'Nox Manga Storage 006',
      SHARD_07: 'Nox Manga Storage 007',
      SHARD_08: 'Nox Manga Storage 008',
      SHARD_09: 'Nox Mangá',
    };

    const existingShards = [
      { skey: 'SHARD_01', id: '3a4be1a3-f5d2-40c9-9eab-697c2357b183', cid: '-1003525800137' },
      { skey: 'SHARD_02', id: 'a3b6a10e-f53a-4873-9f19-d4cc8576de3a', cid: '-1003686965009' },
      { skey: 'SHARD_03', id: '424e8be1-dc8a-4d97-a904-119c7ef1c9b5', cid: '-1004400799763' },
      { skey: 'SHARD_04', id: 'b8fd37d7-3923-4e04-be37-610a1079aa43', cid: '-1004382627509' },
      { skey: 'SHARD_05', id: '80ead41f-7b58-492d-a028-ae0b2669cd93', cid: '-1003889300195' },
      { skey: 'SHARD_06', id: '98701fd5-d376-4310-b664-6aa13bf0cbb1', cid: '-1004356622185' },
      { skey: 'SHARD_07', id: '3535da22-cb50-4b7b-b12f-96c25460d0b6', cid: '-1004413066858' },
      { skey: 'SHARD_08', id: '23518242-ad31-44e9-9997-add190b0a930', cid: '-1004337541258' },
      { skey: 'SHARD_09', id: '935e146d-de3f-4a8e-b393-692944c716fa', cid: '-1004353931378' },
    ];

    for (const es of existingShards) {
      this.shards.push({
        shardId: es.id,
        channelId: es.cid,
        name: shardNames[es.skey] || es.skey,
        activeUploads: 0,
        totalUploads: 0,
        successes: 0,
        failures: 0,
        rateLimits429: 0,
        cooldownUntil: 0,
        maxConcurrent: 3,
        recentUploadTimestamps: [],
        latencies: [],
      });
    }

    for (let i = 10; i <= 21; i++) {
      const skey = `SHARD_${String(i).padStart(2, '0')}`;
      const sdata = cp?.shards?.[skey] || defaultShards10To21[skey];
      if (!sdata) {
        throw new Error(`Missing shard definition for ${skey} in checkpoint`);
      }
      this.shards.push({
        shardId: sdata.shardId,
        channelId: String(sdata.channel_id),
        name: `Nox Manga Storage ${String(i).padStart(3, '0')}`,
        activeUploads: 0,
        totalUploads: 0,
        successes: 0,
        failures: 0,
        rateLimits429: 0,
        cooldownUntil: 0,
        maxConcurrent: 3,
        recentUploadTimestamps: [],
        latencies: [],
      });
    }

    this.logger.info(`DirectTelegramStorage initialized: ${this.bots.length} Bots × ${this.shards.length} Shards operational.`);
  }

  setUploadRate(bytesPerSec: number): void {
    this.bandwidthLimiter.setRate(bytesPerSec);
  }

  getProviderKey(): string {
    return 'telegram';
  }

  getLastBotReference(id?: string): string {
    if (id && this.metadataMap.has(id)) {
      return this.metadataMap.get(id)!.botRef;
    }
    return this.lastBotRef;
  }

  getLastShardId(id?: string): string | null {
    if (id && this.metadataMap.has(id)) {
      return this.metadataMap.get(id)!.shardId;
    }
    return this.lastShardId;
  }

  getLastChannelId(id?: string): string | null {
    if (id && this.metadataMap.has(id)) {
      return this.metadataMap.get(id)!.channelId;
    }
    return this.lastChannelId;
  }

  async healthCheck(): Promise<boolean> {
    const activeBots = this.bots.filter(b => b.cooldownUntil <= Date.now());
    return activeBots.length >= 3 && this.shards.length === 21;
  }

  private cleanRecentWindows(now: number): void {
    const oneMinAgo = now - 60_000;
    for (const b of this.bots) {
      while (b.recentUploadTimestamps.length > 0 && b.recentUploadTimestamps[0] < oneMinAgo) {
        b.recentUploadTimestamps.shift();
      }
    }
    for (const s of this.shards) {
      while (s.recentUploadTimestamps.length > 0 && s.recentUploadTimestamps[0] < oneMinAgo) {
        s.recentUploadTimestamps.shift();
      }
    }
  }

  private async acquireGlobalSlot(): Promise<void> {
    if (this.currentGlobalActive < this.maxGlobalConcurrent) {
      this.currentGlobalActive++;
      return;
    }
    await new Promise<void>((resolve) => {
      this.waitingQueue.push(resolve);
    });
    this.currentGlobalActive++;
  }

  private releaseGlobalSlot(): void {
    this.currentGlobalActive = Math.max(0, this.currentGlobalActive - 1);
    if (this.waitingQueue.length > 0) {
      const next = this.waitingQueue.shift();
      if (next) next();
    }
  }

  private selectOptimalBot(excludeRefs: Set<string> = new Set()): BotRuntimeMetrics {
    const now = Date.now();
    this.cleanRecentWindows(now);

    let candidates = this.bots.filter(b => !excludeRefs.has(b.ref));
    if (candidates.length === 0) {
      candidates = this.bots;
    }

    const healthy = candidates.filter(b => b.cooldownUntil <= now);
    const available = healthy.length > 0 ? healthy : candidates;

    let bestBot = available[0];
    let bestScore = Infinity;

    for (let i = 0; i < available.length; i++) {
      const b = available[i];
      const rrOffset = ((this.botRoundRobinIndex + i) % available.length) * 0.05;
      const score = (b.activeUploads * 10) + (b.recentUploadTimestamps.length * 1.5) + (b.failures * 3) + rrOffset;

      if (score < bestScore) {
        bestScore = score;
        bestBot = b;
      }
    }

    this.botRoundRobinIndex = (this.botRoundRobinIndex + 1) % 10000;
    return bestBot;
  }

  private selectOptimalShard(excludeIds: Set<string> = new Set()): ShardRuntimeMetrics {
    const now = Date.now();
    this.cleanRecentWindows(now);

    let candidates = this.shards.filter(s => !excludeIds.has(s.shardId));
    if (candidates.length === 0) {
      candidates = this.shards;
    }

    const healthy = candidates.filter(s => s.cooldownUntil <= now);
    const available = healthy.length > 0 ? healthy : candidates;

    let bestShard = available[0];
    let bestScore = Infinity;

    for (let i = 0; i < available.length; i++) {
      const s = available[i];
      const rrOffset = ((this.shardRoundRobinIndex + i) % available.length) * 0.05;
      const score = (s.activeUploads * 10) + (s.recentUploadTimestamps.length * 1.0) + (s.failures * 3) + rrOffset;

      if (score < bestScore) {
        bestScore = score;
        bestShard = s;
      }
    }

    this.shardRoundRobinIndex = (this.shardRoundRobinIndex + 1) % 10000;
    return bestShard;
  }

  private executeTelegramUpload(
    token: string,
    channelId: string,
    bytes: Uint8Array,
    id: string
  ): Promise<{ fileId: string; messageId: number }> {
    return new Promise((resolve, reject) => {
      const boundary = `----TelegramUploadBoundary${Date.now()}${Math.random().toString(36).slice(2, 8)}`;
      const headerPart = Buffer.from(
        `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="chat_id"\r\n\r\n` +
        `${channelId}\r\n` +
        `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="disable_content_type_detection"\r\n\r\n` +
        `true\r\n` +
        `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="disable_notification"\r\n\r\n` +
        `true\r\n` +
        `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="document"; filename="${id}.bin"\r\n` +
        `Content-Type: application/octet-stream\r\n\r\n`
      );
      const footerPart = Buffer.from(`\r\n--${boundary}--\r\n`);
      const payloadBuffer = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      const contentLength = headerPart.length + payloadBuffer.length + footerPart.length;

      const req = https.request(
        `https://api.telegram.org/bot${token}/sendDocument`,
        {
          method: 'POST',
          agent: this.httpsAgent,
          headers: {
            'Content-Type': `multipart/form-data; boundary=${boundary}`,
            'Content-Length': String(contentLength),
            'Connection': 'keep-alive',
          },
          timeout: 45_000,
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on('data', (c) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
          res.on('end', () => {
            const raw = Buffer.concat(chunks).toString('utf8');
            try {
              const data = JSON.parse(raw);
              if (res.statusCode === 200 && data.ok && data.result?.document?.file_id) {
                resolve({
                  fileId: data.result.document.file_id,
                  messageId: data.result.message_id || 0,
                });
                return;
              }

              const err: any = new Error(data.description || `Telegram upload failed HTTP ${res.statusCode}`);
              err.statusCode = res.statusCode;
              err.parameters = data.parameters;
              reject(err);
            } catch (jsonErr) {
              const err: any = new Error(`Invalid JSON response from Telegram: ${raw.slice(0, 100)}`);
              err.statusCode = res.statusCode;
              reject(err);
            }
          });
        }
      );

      req.on('timeout', () => {
        req.destroy(new Error(`Upload timed out after 45000ms`));
      });

      req.on('error', (err) => {
        reject(err);
      });

      const writeAsync = async () => {
        if (!req.write(headerPart)) {
          await new Promise<void>((r) => req.once('drain', r));
        }
        const chunkSize = 64 * 1024;
        for (let i = 0; i < payloadBuffer.length; i += chunkSize) {
          const chunk = payloadBuffer.subarray(i, i + chunkSize);
          await this.bandwidthLimiter.acquire(chunk.length);
          if (!req.write(chunk)) {
            await new Promise<void>((r) => req.once('drain', r));
          }
        }
        if (!req.write(footerPart)) {
          await new Promise<void>((r) => req.once('drain', r));
        }
        req.end();
      };
      writeAsync().catch((err) => {
        req.destroy(err);
        reject(err);
      });
    });
  }

  async upload(bytes: Uint8Array, mime: string, id: string, chapterId?: string): Promise<string> {
    await this.acquireGlobalSlot();

    const maxAttempts = 3;
    let lastError: any;
    const excludedBots = new Set<string>();
    const excludedShards = new Set<string>();

    try {
      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const bot = this.selectOptimalBot(excludedBots);
        const shard = this.selectOptimalShard(excludedShards);

        const now = Date.now();
        if (bot.cooldownUntil > now && this.bots.every(b => b.cooldownUntil > now)) {
          const waitMs = Math.min(...this.bots.map(b => b.cooldownUntil)) - now;
          this.logger.warn(`All bots in cooldown. Sleeping ${Math.ceil(waitMs / 1000)}s...`);
          await new Promise(r => setTimeout(r, Math.min(waitMs, 30_000)));
        }

        bot.activeUploads++;
        shard.activeUploads++;
        const startTime = Date.now();

        try {
          const result = await this.executeTelegramUpload(bot.token, shard.channelId, bytes, id);
          const duration = Date.now() - startTime;

          // AIMD Additive Increase on Bot
          bot.activeUploads = Math.max(0, bot.activeUploads - 1);
          bot.totalUploads++;
          bot.successes++;
          bot.consecutiveSuccesses++;
          bot.recentUploadTimestamps.push(Date.now());
          bot.latencies.push(duration);
          if (bot.latencies.length > 50) bot.latencies.shift();

          if (bot.consecutiveSuccesses >= 10 && bot.maxConcurrent < 8) {
            bot.maxConcurrent++;
            bot.consecutiveSuccesses = 0;
          }

          // Shard Success tracking
          shard.activeUploads = Math.max(0, shard.activeUploads - 1);
          shard.totalUploads++;
          shard.successes++;
          shard.recentUploadTimestamps.push(Date.now());
          shard.latencies.push(duration);
          if (shard.latencies.length > 50) shard.latencies.shift();

          // Save metadata
          this.lastBotRef = bot.ref;
          this.lastShardId = shard.shardId;
          this.lastChannelId = shard.channelId;
          this.metadataMap.set(id, {
            botRef: bot.ref,
            shardId: shard.shardId,
            channelId: shard.channelId,
            fileId: result.fileId,
            timestamp: Date.now(),
            byteSize: bytes.byteLength,
            latencyMs: duration,
          });

          // Prevent map overflow
          if (this.metadataMap.size > 2000) {
            const firstKey = this.metadataMap.keys().next().value;
            if (firstKey) this.metadataMap.delete(firstKey);
          }

          return result.fileId;
        } catch (err: any) {
          const duration = Date.now() - startTime;
          bot.activeUploads = Math.max(0, bot.activeUploads - 1);
          shard.activeUploads = Math.max(0, shard.activeUploads - 1);
          bot.failures++;
          shard.failures++;
          lastError = err;

          const is429 = err.statusCode === 429;
          const retryAfter = err.parameters?.retry_after || 15;

          if (is429) {
            bot.rateLimits429++;
            bot.floodWaitSeconds += retryAfter;
            bot.cooldownUntil = Date.now() + (retryAfter + 2) * 1000;
            // AIMD Multiplicative Decrease
            bot.maxConcurrent = Math.max(1, Math.floor(bot.maxConcurrent * 0.5));
            bot.consecutiveSuccesses = 0;

            this.logger.warn(`[FLOOD_WAIT 429] Bot ${bot.ref} throttled for ${retryAfter}s! Failover to alternative bot for attempt ${attempt + 1}/${maxAttempts}...`);
            excludedBots.add(bot.ref);
          } else {
            this.logger.warn(`[UPLOAD_RETRY] Bot ${bot.ref} / Shard ${shard.name} error (${err.message}). Retrying attempt ${attempt + 1}/${maxAttempts}...`);
            excludedBots.add(bot.ref);
            excludedShards.add(shard.shardId);
          }

          if (attempt < maxAttempts) {
            await new Promise(r => setTimeout(r, 200 + Math.random() * 300));
            continue;
          }
        }
      }

      throw lastError || new Error(`DirectTelegramStorage upload failed after ${maxAttempts} attempts`);
    } finally {
      this.releaseGlobalSlot();
    }
  }

  getMetricsSummary() {
    const totalBotUploads = this.bots.reduce((acc, b) => acc + b.totalUploads, 0);
    const totalShardUploads = this.shards.reduce((acc, s) => acc + s.totalUploads, 0);

    const botStats = this.bots.map(b => {
      const pct = totalBotUploads > 0 ? (b.totalUploads / totalBotUploads) * 100 : 0;
      const sortedLat = [...b.latencies].sort((x, y) => x - y);
      const p50 = sortedLat[Math.floor(sortedLat.length * 0.5)] || 0;
      const p95 = sortedLat[Math.floor(sortedLat.length * 0.95)] || 0;
      return {
        bot: b.ref,
        username: b.username,
        uploads: b.totalUploads,
        pct: Number(pct.toFixed(2)),
        successes: b.successes,
        failures: b.failures,
        rateLimits429: b.rateLimits429,
        floodWaitSeconds: b.floodWaitSeconds,
        active: b.activeUploads,
        cooldownUntil: b.cooldownUntil,
        p50LatencyMs: p50,
        p95LatencyMs: p95,
      };
    });

    const shardStats = this.shards.map((s, idx) => {
      const pct = totalShardUploads > 0 ? (s.totalUploads / totalShardUploads) * 100 : 0;
      const sortedLat = [...s.latencies].sort((x, y) => x - y);
      const p50 = sortedLat[Math.floor(sortedLat.length * 0.5)] || 0;
      return {
        shardNum: idx + 1,
        shardId: s.shardId,
        channelId: s.channelId,
        name: s.name,
        uploads: s.totalUploads,
        pct: Number(pct.toFixed(2)),
        successes: s.successes,
        failures: s.failures,
        rateLimits429: s.rateLimits429,
        active: s.activeUploads,
        cooldownUntil: s.cooldownUntil,
        p50LatencyMs: p50,
      };
    });

    return {
      totalBotUploads,
      totalShardUploads,
      bots: botStats,
      shards: shardStats,
    };
  }
}
