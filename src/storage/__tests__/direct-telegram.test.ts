import { describe, expect, it, vi } from 'vitest';
import { DirectTelegramStorageProvider } from '../direct-telegram.js';

describe('DirectTelegramStorageProvider rate-limit boundaries', () => {
  it('cools both the bot and selected channel on a 429 without rerouting that upload', async () => {
    // This exercises the post-selection limiter without reading production
    // tokens/configuration or contacting Telegram.
    const provider = Object.create(DirectTelegramStorageProvider.prototype) as any;
    const bot = {
      ref: 'bot-a', token: 'redacted', activeUploads: 0, totalUploads: 0, successes: 0, failures: 0,
      rateLimits429: 0, floodWaitSeconds: 0, cooldownUntil: 0, maxConcurrent: 4, consecutiveSuccesses: 0,
      recentUploadTimestamps: [], latencies: [],
    };
    const shard = {
      shardId: 'shard-a', channelId: 'channel-a', name: 'channel-a', activeUploads: 0, totalUploads: 0,
      successes: 0, failures: 0, rateLimits429: 0, cooldownUntil: 0, maxConcurrent: 3,
      recentUploadTimestamps: [], latencies: [],
    };
    const flood = Object.assign(new Error('Too Many Requests'), { statusCode: 429, parameters: { retry_after: 3 } });
    provider.acquireGlobalSlot = vi.fn(async () => {});
    provider.releaseGlobalSlot = vi.fn();
    provider.reserveDestination = vi.fn(async () => {
      bot.activeUploads++;
      shard.activeUploads++;
      return { bot, shard };
    });
    provider.executeTelegramUpload = vi.fn(async () => { throw flood; });
    provider.logger = { warn: vi.fn() };

    const before = Date.now();
    await expect(provider.upload(Uint8Array.of(1), 'image/png', 'media-id')).rejects.toMatchObject({ statusCode: 429, retryAfter: 3 });

    expect(provider.executeTelegramUpload).toHaveBeenCalledTimes(1);
    expect(bot.rateLimits429).toBe(1);
    expect(shard.rateLimits429).toBe(1);
    expect(bot.cooldownUntil).toBeGreaterThanOrEqual(before + 5_000);
    expect(shard.cooldownUntil).toBe(bot.cooldownUntil);
    expect(provider.releaseGlobalSlot).toHaveBeenCalledOnce();
  });
});
